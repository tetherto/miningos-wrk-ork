'use strict'

const async = require('async')
const TetherWrkBase = require('tether-wrk-base/workers/base.wrk.tether')
const debug = require('debug')('ork:aggr')
const gLibStats = require('miningos-lib-stats')
const mingo = require('mingo')
const ActionCaller = require('./lib/action.caller')
const { cloneDeep, isNil, isEmpty } = require('@bitfinex/lib-js-util-base')
const { differenceInDays, addDays } = require('date-fns')
const { daysTo24HrIntervals, isCurrentDay, sortThings } = require('./lib/util')
const {
  OPTIONAL_CONFIGS,
  RPC_METHODS,
  COMMENT_ACTION,
  INVALID_ACTIONS_ERRORS,
  DEFAULT_TIMEZONE
} = require('./lib/constants')
const aggrCrossthg = require('./lib/aggr.crossthg')
const { setTimeout: sleep } = require('timers/promises')

class WrkProcAggr extends TetherWrkBase {
  constructor (conf, ctx) {
    if (!ctx.cluster) {
      throw new Error('ERR_PROC_RACK_UNDEFINED')
    }
    ctx.rack = ctx.cluster // new base class uses rack arg
    super(conf, ctx)

    this.prefix = `${this.wtype}-${ctx.cluster}`

    this.loadConf('base.ork', 'ork')
    this._loadOptionalConfigs()

    this.init()
    this.start()
  }

  _loadOptionalConfigs () {
    OPTIONAL_CONFIGS.forEach(config => {
      try {
        this.loadConf(config.name, config.key)
      } catch (e) {
        this.debugError(`failed to load config ${config.name}`, e)
      }
    })
  }

  init () {
    super.init()

    this.setInitFacs([
      ['fac', 'bfx-facs-interval', '0', '0', {}, -10],
      ['fac', 'bfx-facs-scheduler', '0', '0', {}, -10],
      ['fac', 'hp-svc-facs-store', 's1', 's1', {
        storeDir: `store/${this.ctx.cluster}-db`
      }, -5],
      ['fac', 'bfx-facs-lru', 'r0', 'r0', {
        maxAge: 900000,
        max: 100000
      }, 0],
      ['fac', 'svc-facs-action-approver', '0', '0', {}, 20]
    ])

    this.mem = {
      crossActionsLRU: {}
    }
    this.savedAggrKeys = {}
  }

  debugGeneric (msg) {
    debug(`[STORE/${this.ctx.cluster}]`, ...arguments)
  }

  debugError (data, e, alert = false) {
    if (alert) {
      return console.error(`[STORE/${this.ctx.cluster}]`, data, e)
    }
    debug(`[STORE/${this.ctx.cluster}]`, data, e)
  }

  async registerRack (req) {
    if (!req.id) {
      throw new Error('ERR_RACK_ID_INVALID')
    }

    if (!req.type) {
      throw new Error('ERR_RACK_TYPE_INVALID')
    }

    const info = req.info

    if (!info.rpcPublicKey) {
      throw new Error('ERR_RACK_INFO_RPC_PUBKEY_INVALID')
    }

    await this.racks.put(
      req.id,
      Buffer.from(JSON.stringify(req))
    )

    return 1
  }

  async forgetRacks (req) {
    const stream = this.racks.createReadStream()
    let cnt = 0

    for await (const data of stream) {
      const entry = JSON.parse(data.value.toString())

      let valid = false

      if (Array.isArray(req.ids)) {
        if (req.ids.includes(entry.id)) {
          valid = true
        }
      }

      if (req.all) {
        valid = true
      }

      if (valid) {
        await this.racks.del(entry.id)
        cnt++
      }
    }

    return cnt
  }

  async listRacks (req) {
    if (req.type && typeof req.type !== 'string') {
      throw new Error('ERR_TYPE_INVALID')
    }

    const stream = this.racks.createReadStream()
    const res = []

    for await (const data of stream) {
      const entry = JSON.parse(data.value.toString())

      if (!req.type || entry.type.startsWith(req.type)) {
        res.push(entry)
      }
    }

    if (!req.keys) {
      return res.map(entry => {
        delete entry.info.rpcPublicKey
        return entry
      })
    }

    return res
  }

  async listThings (req) {
    const stream = this.racks.createReadStream()

    const collection = await Array.prototype.concat.apply([], await async.mapLimit(stream, 25, async data => {
      const entry = JSON.parse(data.value.toString())
      try {
        return await this.net_r0.jRequest(
          entry.info.rpcPublicKey,
          'listThings',
          req, { timeout: 10000 }
        )
      } catch (e) {
        this.debugError(`listThings ${entry.id}`, e, true)
        return []
      }
    }))

    if (!req.sort) {
      return collection
    }
    if (!Array.isArray(collection)) {
      return []
    }
    return collection.sort((a, b) => sortThings(a, b, req.sort))
  }

  async getHistoricalLogs (req) {
    if (!req.logType) {
      throw new Error('ERR_LOG_TYPE_INVALID')
    }
    const stream = this.racks.createReadStream()

    const collection = await Array.prototype.concat.apply([], await async.mapLimit(stream, 25, async data => {
      const entry = JSON.parse(data.value.toString())
      try {
        return await this.net_r0.jRequest(
          entry.info.rpcPublicKey,
          'getHistoricalLogs',
          req, { timeout: 10000 }
        )
      } catch (e) {
        this.debugError(`${req.logType} ${entry.id}`, e, true)
        return []
      }
    }))

    if (!req.sort) {
      return collection
    }

    const query = new mingo.Query({})
    const cursor = query.find(collection).sort(req.sort)
    return cursor.all()
  }

  async forgetThings (req) {
    const stream = this.racks.createReadStream()

    const collection = await Array.prototype.concat.apply([], await async.mapLimit(stream, 25, async data => {
      const entry = JSON.parse(data.value.toString())

      return this.net_r0.jRequest(
        entry.info.rpcPublicKey,
        'forgetThings',
        req, { timeout: 10000 }
      )
    }))

    return collection.reduce((acc, e) => {
      return acc + e
    }, 0)
  }

  async aggregateTailLogs () {
    if (this.aggregatingTailLogs) return
    this.aggregatingTailLogs = true

    try {
    // fetch and save aggr data for previous days
      let days = this.conf.aggrData.aggrTailLogStoreDays || 180
      const aggrTimezones = this.conf.globalConfig?.aggrTailLogTimezones || [{ code: DEFAULT_TIMEZONE, offset: 0 }]
      const timezoneIntervals = {}
      aggrTimezones.forEach(timezone => {
        // skip fetching data for previous days already fetched, unless next day begins
        if (this.lastFetchedAggrLogs && isCurrentDay(this.lastFetchedAggrLogs, timezone.offset)) {
          days = 1 // fetch only today's data
        }

        // break down days into 24 hour intervals
        timezoneIntervals[timezone.code] = daysTo24HrIntervals(days, timezone.offset)
      })

      // calculate tail-log aggregated data for each key/type
      const aggrTailLogKeys = this.conf.aggrData.aggrTailLogKeys || []
      for (const aggrTailLogKey of aggrTailLogKeys) {
        for (const [timezone, intervals] of Object.entries(timezoneIntervals)) {
          await this._saveAggrTaillogs(aggrTailLogKey, intervals, timezone)
        }
      }
      this.lastFetchedAggrLogs = Date.now()
    } catch (e) {
      this.debugError('aggregateTailLogs err', e)
    } finally {
      this.aggregatingTailLogs = false
    }
  }

  async _saveAggrTaillogs (aggrTailLogKey, intervals, timezone) {
    const { key, type, tag, fields, aggrFields, limit } = aggrTailLogKey
    for (const { startTs: start, endTs: end, index } of intervals) {
      // check if interval already fetched
      const aggrLogKey = `tail-log-${type}-${timezone}-day-${index}`
      const logIntervalKey = `${type}-${start}-${end}`
      let data = await this._getCachedAggrData(logIntervalKey)
      if (!data?.length) {
        // data is fetched in bg, add delay between calls
        await sleep(this.conf.aggrData.aggrTailLogApiDelay || 1000)
        data = await this.tailLog(this._getTailLogAggrParams({ key, type, tag, fields, aggrFields, start, end, limit }))
      }

      // save 24 hour aggregated tail-log data in db
      await this.tailLogAggrDb.put(aggrLogKey, Buffer.from(JSON.stringify(data)))
      if (index) this.savedAggrKeys[logIntervalKey] = aggrLogKey
    }
  }

  async _getCachedAggrData (logIntervalKey) {
    if (this.savedAggrKeys[logIntervalKey]) {
      const data = await this.tailLogAggrDb.get(this.savedAggrKeys[logIntervalKey])
      if (data) return JSON.parse(data.value.toString())
    }
    return null
  }

  _getTailLogAggrParams (opts) {
    const { key, type, tag, fields, aggrFields, start, end, limit } = opts
    return {
      key,
      type,
      tag,
      ...(fields ? { fields } : {}),
      ...(aggrFields ? { aggrFields } : {}),
      start,
      end,
      limit,
      aggrTimes: [{ start, end }]
    }
  }

  async tailLogCustomRangeAggr (req) {
    // check configs loaded
    if (!this.conf.aggrData || !this.conf.aggrStats) {
      throw new Error('ERR_NOT_CONFIGURED')
    }

    if (!Array.isArray(req.keys)) throw new Error('ERR_KEYS_INVALID')

    const aggrData = []
    for (const key of req.keys) {
      try {
        const data = await this._getTailLogRangeAggr(key)
        aggrData.push({ type: key.type, data, error: null })
      } catch (e) {
        this.debugError(`tailLogCustomRangeAggr ${key}`, e)
        aggrData.push({ type: key.type, data: null, error: e.message })
      }
    }

    return aggrData
  }

  _validateTaillogAggrReq (req) {
    const { type, startDate, endDate } = req

    if (!type) {
      throw new Error('ERR_TYPE_INVALID')
    }

    if (!startDate || isNaN(new Date(startDate).getTime())) {
      throw new Error('ERR_START_DATE_INVALID')
    }

    if (!endDate || isNaN(new Date(endDate).getTime())) {
      throw new Error('ERR_END_DATE_INVALID')
    }
  }

  _getTaillogAggrOps (logKey, type) {
    const state = { ops: {} }
    if (logKey.ops) {
      state.ops = logKey.ops
    } else {
      const ops = this.conf.aggrStats[type].ops
      for (const op in ops) {
        state.ops[op] = { src: op, op: ops[op].op }
      }
    }
    return state
  }

  async _getTailLogRangeAggr (req) {
    this._validateTaillogAggrReq(req)
    const { type, startDate, endDate, timezoneOffset = 0, fields = {} } = req

    // find timezone from offset
    const aggrTimezones = this.conf.globalConfig?.aggrTailLogTimezones || [{ code: DEFAULT_TIMEZONE, offset: 0 }]
    const timezone = aggrTimezones.find(t => t.offset === timezoneOffset)
    if (!timezone) throw new Error('ERR_TIMEZONE_OFFSET_INVALID')

    // fetch saved aggregated tail-logs within date range
    let aggrData = {}
    const aggrTailLogKeys = this.conf.aggrData.aggrTailLogKeys || []
    const logKey = aggrTailLogKeys.find(t => t.type === type)
    if (!logKey) return {}

    // zero index for most recent date, so start-date gives end-index
    const endIndex = differenceInDays(new Date(), new Date(startDate))
    const startIndex = differenceInDays(new Date(), new Date(endDate))

    // set aggregated keys for the ops
    const state = this._getTaillogAggrOps(logKey, type)

    // calculate aggrCount and aggrIntervals
    let aggrCount = 0
    let aggrIntervals = 0
    const dailyDataObjects = []
    const monthlyData = {}
    let currMonth
    let prevMonth

    // fetch daily aggr tail-log from db, aggr it further for the date range
    for (let i = startIndex; i <= endIndex; i++) {
      const ts = addDays(startDate, (i - startIndex)).getTime()
      let data = await this.tailLogAggrDb.get(`tail-log-${type}-${timezone.code}-day-${i}`)
      if (data) {
        data = JSON.parse(data.value.toString())

        // create arr of objects instead of aggregated data
        if (req.shouldReturnDailyData) {
          dailyDataObjects.push({ ts, val: this._projection(data, fields)[0] })
          continue
        }
        if (req.shouldReturnMonthlyData) {
          // group data by month
          prevMonth = currMonth
          currMonth = new Date(ts).getMonth() + 1
          if (!monthlyData[currMonth]) {
            if (prevMonth && !isEmpty(aggrData)) {
              gLibStats.tallyStats(state, aggrData)
              monthlyData[prevMonth] = cloneDeep(aggrData)
            }
            aggrData = {}
          }
        }

        for (const entry of data) {
          gLibStats.applyStats(state, aggrData, entry)

          aggrCount += entry.aggrCount || 0
          aggrIntervals += entry.aggrIntervals || 0
        }
        aggrData = this._projection([aggrData], logKey.aggrFields)[0]
      }
    }

    if (req.shouldReturnDailyData) {
      return dailyDataObjects
    }
    if (req.shouldReturnMonthlyData) {
      return Object.entries(monthlyData).map(e => ({ month: parseInt(e[0]), val: e[1] }))
    }

    gLibStats.tallyStats(state, aggrData)
    return { ...aggrData, aggrCount, aggrIntervals }
  }

  _projection (data, fields = {}) {
    const query = new mingo.Query({})
    const cursor = query.find(data, fields)
    return cursor.all()
  }

  _taillogProjections (req, specs) {
    const { aggrFields = {}, fields = {} } = req
    for (const field in aggrFields) {
      if (specs.ops[field]?.src) {
        fields[specs.ops[field].src] = aggrFields[field]
      }
    }
    // always read ts and aggrTsRange and aggrCount and aggrIntervals
    if (Object.keys(fields).length && !fields.ts) fields.ts = 1
    if (Object.keys(aggrFields).length && !aggrFields.ts) aggrFields.ts = 1
    if (Object.keys(aggrFields).length && !aggrFields.aggrTsRange) aggrFields.aggrTsRange = 1
    if (Object.keys(aggrFields).length && !aggrFields.aggrCount) aggrFields.aggrCount = 1
    if (Object.keys(aggrFields).length && !aggrFields.aggrIntervals) aggrFields.aggrIntervals = 1

    return { fields, aggrFields }
  }

  async _racksForType (type) {
    const stream = this.racks.createReadStream()
    const racks = []
    for await (const data of stream) {
      const entry = JSON.parse(data.value.toString())
      if (entry.type === type || entry.type.startsWith(`${type}-`)) {
        racks.push(entry)
      }
    }
    return racks
  }

  _getTaillogSpecs (type) {
    // check optional config loaded
    if (!this.conf.aggrStats) {
      throw new Error('ERR_NOT_CONFIGURED')
    }

    if (!type) {
      throw new Error('ERR_TYPE_INVALID')
    }

    const specs = this.conf.aggrStats
    const btype = type.split('-')[0]

    if (!specs[btype]) {
      throw new Error('ERR_TYPE_AGGR_INVALID')
    }

    return specs[btype]
  }

  async tailLog (req) {
    const specs = this._getTaillogSpecs(req.type)
    const { aggrFields, fields } = this._taillogProjections(req, specs)
    // set fields to fetch from racks
    req.fields = fields

    const racks = await this._racksForType(req.type)

    let res = Array.prototype.concat.apply(
      [],
      await async.mapLimit(racks, 25, async rack => {
        try {
          return await this.net_r0.jRequest(
            rack.info.rpcPublicKey,
            'tailLog',
            req, { timeout: 10000 }
          )
        } catch (e) {
          this.debugError(`tailLog ${rack.id} type:${req.type} key:${req.key} tag:${req.tag}`, e, true)
          return []
        }
      })
    )

    const aggrTimes = req.aggrTimes
    res = res.reduce((acc, item) => {
      let grps = [item.ts]
      if (aggrTimes?.length) {
        const ranges = aggrTimes.filter(val => item.ts >= val.start && item.ts <= val.end)
        grps = ranges.map(range => `${range.start}-${range.end}`)
      }

      grps.forEach(grp => {
        acc[grp] = acc[grp] ?? []

        // aggrTimes ranges can share object instances, make a copy to avoid calc overlap
        if (aggrTimes?.length > 1) {
          item = cloneDeep(item)
        }

        acc[grp].push(item)
      })

      return acc
    }, {})

    const state = {}
    const aggr = []

    state.ops = specs.ops

    try {
      for (const k in res) {
        const tf = res[k]
        if (!tf.length) {
          continue
        }

        const acc = {
          ts: aggrTimes?.length ? 0 : tf[0].ts,
          aggrTsRange: aggrTimes?.length ? k : '',
          aggrCount: tf.length
        }

        // find unique intervals count
        const uniqueIntervals = new Set()

        for (const entry of tf) {
          gLibStats.applyStats(state, acc, entry)

          // filter out objects with just ts property
          if (Object.keys(entry).length > 1) {
            uniqueIntervals.add(entry.ts)
          }
        }
        acc.aggrIntervals = uniqueIntervals.size

        gLibStats.tallyStats(state, acc)

        aggr.push(acc)
      }
    } catch (e) {
      this.debugGeneric(e)
    }

    aggr.sort((a, b) => {
      return a.ts - b.ts
    })

    // check if cross thing aggregation type
    if (req.applyAggrCrossthg && this.conf.aggrCrossthg?.[req.type]) {
      await aggrCrossthg.call(this, aggr, this.conf.aggrCrossthg[req.type])
    }

    return this._projection(aggr, aggrFields)
  }

  async tailLogMulti (req) {
    const { keys, ...reqParams } = req

    if (!Array.isArray(keys)) throw new Error('ERR_KEYS_INVALID')

    const missingType = keys.find(val => !val.type)
    if (missingType) throw new Error('ERR_TYPE_INVALID')

    return await async.mapLimit(
      keys,
      5,
      async key => await this.tailLog({ ...key, ...reqParams })
    )
  }

  async _thgCommentsAction (req, methodName) {
    if (!req.rackId) throw new Error('ERR_RACK_ID_INVALID')
    if (!req.thingId) throw new Error('ERR_THING_ID_INVALID')

    const rack = await this.racks.get(req.rackId)
    if (!rack) return 0

    const entry = JSON.parse(rack.value.toString())
    try {
      return await this.net_r0.jRequest(
        entry.info.rpcPublicKey,
        methodName,
        req,
        { timeout: 10000 }
      )
    } catch (e) {
      this.debugError(`${methodName} ${entry.id}`, e, true)
      return 0
    }
  }

  async saveThingComment (req) {
    return this._thgCommentsAction(req, COMMENT_ACTION.ADD)
  }

  async editThingComment (req) {
    return this._thgCommentsAction(req, COMMENT_ACTION.EDIT)
  }

  async deleteThingComment (req) {
    return this._thgCommentsAction(req, COMMENT_ACTION.DELETE)
  }

  async pushActionsBatch (req) {
    const { batchActionsPayload, voter, authPerms } = req
    let { batchActionUID, suffix } = req
    const batchTs = Date.now()
    if (batchActionUID) batchActionUID = `${batchTs}-${batchActionUID}`
    if (suffix) batchActionUID = `${batchActionUID}-${suffix}`
    if (!Array.isArray(batchActionsPayload)) {
      throw new Error('ERR_PAYLOAD_INVALID')
    }

    return await async.mapLimit(
      batchActionsPayload,
      5,
      async ({ query, action, params, rackType = null }) => {
        return await this.pushAction({
          query,
          action,
          params,
          voter,
          authPerms,
          ...(batchActionUID ? { batchActionUID } : {}),
          rackType
        })
      }
    )
  }

  async _shouldSkipRackType (type, rack) {
    if (!type) return false
    const rackEntry = await this.racks.get(rack)
    const rackData = rackEntry ? JSON.parse(rackEntry.toString()) : null
    return (rackData && rackData.type !== type && !rackData.type.startsWith(`${type}-`))
  }

  async pushAction (req) {
    const { query, action, params, voter, authPerms, batchActionUID, rackType = null } = req

    const { targets, requiredPerms } = await this.actionCaller.getWriteCalls(query, action, params, authPerms)

    let reqVotes = 1
    let callCount = 0
    const errors = []
    for (const rack in targets) {
      if (await this._shouldSkipRackType(rackType, rack)) continue

      const entry = targets[rack]
      reqVotes = entry.reqVotes > reqVotes ? entry.reqVotes : reqVotes
      delete entry.reqVotes
      callCount += entry.calls.length

      if (entry.error && INVALID_ACTIONS_ERRORS.some(err => entry.error.includes(err))) {
        continue
      }

      if (!entry.calls.length) {
        this.debugError({ rack, entry }, 'ERR_ORK_ACTION_CALLS_EMPTY')
        errors.push(`${rack}: ${entry.error || 'ERR_ORK_ACTION_CALLS_EMPTY'}`)
      }
    }
    if (callCount === 0) {
      errors.push('ERR_ORK_ACTION_CALLS_EMPTY')
      return { id: null, errors }
    }

    const data = await this.actionApprover_0.pushAction({
      action,
      payload: [params, targets, requiredPerms],
      voter,
      reqVotesPos: reqVotes,
      // All the actions would denied by single negative vote.
      reqVotesNeg: 1,
      batchActionUID
    })
    return { id: data.id, data, errors }
  }

  async getGlobalConfig (req) {
    const fields = req.fields || {}
    if (!this.conf?.globalConfig) {
      throw new Error('ERR_GLOBAL_CONFIG_MISSING')
    }
    return this._projection([this.conf.globalConfig], fields)?.[0]
  }

  async getWrkConf (req) {
    if (!req.type) {
      throw new Error('ERR_TYPE_INVALID')
    }
    const stream = this.racks.createReadStream()
    const racks = []
    const result = []
    for await (const data of stream) {
      const entry = JSON.parse(data.value.toString())
      if (entry.type === req.type || entry.type.startsWith(`${req.type}-`)) {
        racks.push(entry)
      }
    }

    await async.eachLimit(racks, 25, async (rack) => {
      try {
        const config = await this.net_r0.jRequest(
          rack.info.rpcPublicKey,
          'getWrkConf',
          req,
          { timeout: 10000 }
        )
        result.push({ rackId: rack.id, config })
      } catch (e) {
        this.debugError(`getWrkConf ${rack.id}`, e, true)
        result.push({ rackId: rack.id, config: null })
      }
    })

    return result
  }

  async getThingConf (req) {
    if (!req.type) {
      throw new Error('ERR_TYPE_INVALID')
    }
    const stream = this.racks.createReadStream()
    const racks = []
    const result = []
    for await (const data of stream) {
      const entry = JSON.parse(data.value.toString())
      if (entry.type === req.type || entry.type.startsWith(`${req.type}-`)) {
        racks.push(entry)
      }
    }

    await async.eachLimit(racks, 25, async (rack) => {
      try {
        const requestValue = await this.net_r0.jRequest(
          rack.info.rpcPublicKey,
          'getThingConf',
          req,
          { timeout: 10000 }
        )
        result.push({ rackId: rack.id, requestValue })
      } catch (e) {
        this.debugError(`getThingConf ${rack.id}`, e, true)
        result.push({ rackId: rack.id, config: null })
      }
    })

    return result
  }

  async getAction (req) {
    const { id, type } = req
    const { data } = await this.actionApprover_0.getAction(type, id)
    // split targets, call params and required perms
    data.requiredPerms = data.payload[2]
    data.targets = data.payload[1]
    data.params = data.payload[0]
    delete data.payload

    return data
  }

  async getActionsBatch (req) {
    const { ids } = req

    const res = await Promise.all(ids.map(async (id) => {
      const types = ['voting', 'ready', 'executing', 'done']
      const queryRes = await Promise.allSettled(types.map(t => this.actionApprover_0.getAction(t, id)))

      for (let i = 0; i < types.length; i++) {
        const type = types[i]
        const entry = queryRes[i]
        if (entry.status === 'fulfilled') {
          const action = entry.value.data
          // split targets, call params and required perms
          action.requiredPerms = action.payload[2]
          action.targets = action.payload[1]
          action.params = action.payload[0]
          delete action.payload
          return { type, action }
        }
      }

      return null
    }))

    return res.filter(Boolean)
  }

  async cancelActionsBatch (req) {
    const { ids, voter } = req
    return await this.actionApprover_0.cancelActionsBatch({ ids, voter })
  }

  async voteAction (req) {
    const { id, voter, approve, authPerms } = req
    const { data } = await this.actionApprover_0.getAction('voting', id)
    const requiredPerms = data.payload[2]
    if (!requiredPerms.every(p => authPerms.includes(p))) {
      throw new Error('ERR_ACTION_DENIED')
    }

    await this.actionApprover_0.voteAction({ id, voter, approve })
    return 1
  }

  /**
   * Retrieves all actions associated with a given batch UID by querying within a small time window around its timestamp.
   *
   * @private
   * @async
   * @param {string} batchUID - The batch UID string, where the timestamp is expected to be the first part before a dot (e.g., "1747137700278.something").
   * @param {"voting" | "done" | "ready" | "executing"} type - The type of actions to query for.
   * @returns {Promise<Array<Object>>} - A Promise that resolves to an array of action objects retrieved from the query.
   *
   * @example
   * const actions = await _getActionsByBatchUID("1747137700278.someId", "done");
   */
  async _getActionsByBatchUID (batchUID, type) {
    const timestamp = batchUID.split('-')[0]
    const range = {
      gte: Number(timestamp),
      lte: Number(timestamp) + 2 * 60 * 1000
    }
    const queryStream = this.actionApprover_0.query(type, range)
    return await this._getActionsFromQueryStream(queryStream)
  }

  async _getActionsFromQueryStream (queryStream) {
    const res = []
    for await (const entry of queryStream) {
      // split targets, call params and required perms
      entry.requiredPerms = entry.payload[2]
      entry.targets = entry.payload[1]
      entry.params = entry.payload[0]
      delete entry.payload
      for (const target of Object.values(entry.targets)) {
        target.calls?.forEach(call => {
          delete call.tags
        })
      }

      res.push(entry)
    }
    return res
  }

  _getOneActionPerBatch (actions) {
    const existingBatch = new Set()
    const result = []
    for (const action of actions) {
      if (!action.batchActionUID) {
        result.push(action)
      } else if (!existingBatch.has(action.batchActionUID)) {
        existingBatch.add(action.batchActionUID)
        result.push(action)
      }
    }
    return result
  }

  _filterInvalidActionsErrors (actions) {
    return actions.filter(a => {
      const targets = a.targets || {}
      for (const rack in targets) {
        const entry = targets[rack]
        if (!entry.error || !INVALID_ACTIONS_ERRORS.some(err => entry.error.includes(err))) {
          return true
        }
      }
      return false
    })
  }

  async _groupBatchActions (filteredActions, type) {
    const groupedActions = []
    await async.mapLimit(
      filteredActions,
      10,
      async (action) => {
        const batchActionUID = action.batchActionUID
        if (batchActionUID) {
          const actions = await this._getActionsByBatchUID(batchActionUID, type)
          groupedActions.push({
            batchActionUID,
            id: batchActionUID.split('-')[0],
            actions
          })
        } else {
          groupedActions.push(action)
        }
      }
    )
    return groupedActions
  }

  /**
   * Queries actions from the action approver by a specified type and filter.
   * Optionally groups the results by `batchActionUID` if `groupBatch` is true.
   *
   * @private
   * @async
   * @param {"voting" | "done" | "ready" | "executing"} type - The type of actions to retrieve.
   * @param {{
   *   gt?: number,
   *   gte?: number,
   *   lt?: number,
   *   lte?: number
   * }} filter - A filter object defining the range criteria for the query.
   * @param {{
   *   limit?: number
   * }} opts - Additional options for the query, such as a result limit.
   * @param {boolean} [groupBatch=false] - Whether to group the results by batch.
   * @returns {Promise<Array<Object> | Array<{
   *   batchActionUID: string,
   *   id: string,
   *   actions: Array<Object>
   * }>>} - Returns a Promise that resolves to either a flat array of action objects or an array of grouped batch objects.
   *
   * @example
   * await _queryActionsByType('voting', { gt: 1747137700000 }, { limit: 100 }, true)
   */
  async _queryActionsByType (type, filter, opts, groupBatch = false) {
    const queryStream = this.actionApprover_0.query(type, filter, opts)
    const res = await this._getActionsFromQueryStream(queryStream)
    if (!groupBatch) return res
    let filteredActions = this._getOneActionPerBatch(res)
    if (type === 'done') {
      filteredActions = this._filterInvalidActionsErrors(filteredActions)
    }
    return await this._groupBatchActions(filteredActions, type)
  }

  _filterData (data, req = {}) {
    if (!isNil(req.query) || !isNil(req.fields)) {
      const query = new mingo.Query(req.query || {})
      data = query.find(data, req.fields || {}).all()
    }
    return data
  }

  async queryActions (req) {
    const { queries, suffix, groupBatch = false } = req

    if (!Array.isArray(queries)) {
      throw new Error('ERR_QUERIES_INVALID')
    }

    queries.forEach(query => {
      if (!query.type || typeof query.type !== 'string') {
        throw new Error('ERR_QUERIES_TYPE_INVALID')
      }
    })
    const res = {}
    await async.mapLimit(queries, 25, async ({ type, filter, opts, query, fields }) => {
      const actions = await this._queryActionsByType(type, filter, opts, groupBatch)

      let finalQuery = query || {}
      if (suffix) {
        const escapedSuffix = suffix.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
        finalQuery = {
          ...finalQuery,
          batchActionUID: { $regex: new RegExp(`${escapedSuffix}$`) }
        }
      }

      res[type] = this._filterData(actions, { query: finalQuery, fields })
    })
    return res
  }

  async getWrkExtData (req) {
    if (!req.type) {
      throw new Error('ERR_TYPE_INVALID')
    }

    const stream = this.racks.createReadStream()
    const racks = []

    for await (const data of stream) {
      const entry = JSON.parse(data.value.toString())
      if (entry.type === req.type || entry.type.startsWith(`${req.type}-`)) {
        racks.push(entry)
      }
    }

    let res = Array.prototype.concat.apply(
      [],
      await async.mapLimit(racks, 25, async rack => {
        try {
          return await this.net_r0.jRequest(
            rack.info.rpcPublicKey,
            'getWrkExtData',
            req, { timeout: 10000 }
          )
        } catch (e) {
          this.debugError(`getWrkExtData ${rack.id}`, e, true)
          return []
        }
      })
    )

    let specs = this.conf.aggrStats
    const btype = req.type.split('-')[0]
    if (!specs[btype]) return res

    specs = specs[btype]
    res = res.reduce((acc, item) => {
      const grps = [item.ts]
      grps.forEach(grp => {
        acc[grp] = acc[grp] ?? []
        acc[grp].push(item)
      })
      return acc
    }, {})

    const state = { ops: specs.ops }
    const aggr = []
    for (const k in res) {
      const tf = res[k]
      if (!tf.length) continue
      const acc = { ts: k }
      for (const entry of tf) {
        gLibStats.applyStats(state, acc, entry)
      }
      gLibStats.tallyStats(state, acc)
      aggr.push(acc)
    }

    // check if cross thing aggregation type
    if (req.applyAggrCrossthg && this.conf.aggrCrossthg?.[req.type]) {
      await aggrCrossthg.call(this, aggr, this.conf.aggrCrossthg[req.type])
    }

    return aggr
  }

  async crossAggrActions () {
    if (!this.conf.globalConfig?.isAutoSleepAllowed) return
    const conf = this.conf.crossAggrAction
    if (!conf) return
    const {
      tagPrefix,
      crossThingType,
      action,
      voter,
      params,
      authPerms,
      listThingsReq,
      crossThingsActionReq
    } = conf

    if (
      !tagPrefix ||
      !crossThingType ||
      !action ||
      !params ||
      !Array.isArray(params) ||
      !authPerms ||
      !Array.isArray(authPerms) ||
      !crossThingsActionReq
    ) {
      this.debugError('ERR_CROSS_AGGR_ACTIONS_INVALID_CONFIG')
      return
    }
    const listThingsResponse = await this.listThings(listThingsReq)
    if (!Array.isArray(listThingsResponse)) {
      this.debugError('ERR_CROSS_AGGR_ACTIONS_INVALID_LIST_THINGS_RESPONSE')
      return
    }

    await async.eachLimit(listThingsResponse, 25, async (thing) => {
      if (!thing.tags || !Array.isArray(thing.tags)) {
        this.debugError('ERR_CROSS_AGGR_ACTIONS_THING_TAGS_INVALID')
        return
      }
      const tag = thing.tags.find((tag) => tag.includes(tagPrefix))
      if (!tag) return
      const crossThingDevices = [
        { tags: { $in: [`t-${crossThingType}`] } },
        { tags: { $in: [tag] } }
      ]
      const query = {
        $and: crossThingDevices
      }
      const now = Date.now()
      const lastActionAt = this.mem.crossActionsLRU[`${crossThingType}_${tag}`]
      if ((now - lastActionAt) < (this.conf.crossAggrAction.minActionTsDiff || 60000)) {
        return
      }
      const thingsNeedAction = await this.listThings({
        query: {
          $and: [
            ...crossThingDevices,
            ...crossThingsActionReq.devicesCondition
          ]
        },
        fields: crossThingsActionReq.fields
      })
      if (!thingsNeedAction?.length) return
      try {
        await this.pushAction({
          query,
          action,
          params,
          voter: voter || 'MOS AUTOMATION',
          authPerms
        })
        this.mem.crossActionsLRU[`${crossThingType}_${tag}`] = now
      } catch (e) {
        this.debugError('ERR_CROSS_AGGR_ACTIONS_PUSH_ACTION', e, true)
      }
    })
  }

  async getWrkSettings (req) {
    if (!req.rackId || (typeof req.rackId !== 'string')) throw new Error('ERR_RACK_ID_INVALID')

    const rack = await this.racks.get(req.rackId)
    if (!rack) return 0

    const entry = JSON.parse(rack.value.toString())
    try {
      return await this.net_r0.jRequest(
        entry.info.rpcPublicKey,
        'getWrkSettings',
        req, { timeout: 10000 }
      )
    } catch (e) {
      this.debugError(`getWrkSettings ${req.rackId}`, e, true)
      throw new Error('ERR_GET_SETTINGS_FAILED')
    }
  }

  async saveWrkSettings (req) {
    if (!req.rackId || (typeof req.rackId !== 'string')) throw new Error('ERR_RACK_ID_INVALID')
    if (!req.entries || (typeof req.entries !== 'object')) throw new Error('ERR_ENTRIES_INVALID')

    const rack = await this.racks.get(req.rackId)
    if (!rack) return 0

    const entry = JSON.parse(rack.value.toString())
    try {
      return await this.net_r0.jRequest(
        entry.info.rpcPublicKey,
        'saveWrkSettings',
        req, { timeout: 10000 }
      )
    } catch (e) {
      this.debugError(`saveWrkSettings ${req.rackId}`, e, true)
      throw new Error('ERR_SAVE_SETTINGS_FAILED')
    }
  }

  async setGlobalConfig (req) {
    if (isEmpty(req)) throw new Error('ERR_CONFIG_INVALID')
    if (!this.conf.globalConfig) throw new Error('ERR_GLOBAL_CONFIG_NOT_FOUND')
    if (!isNil(req.isAutoSleepAllowed) && typeof req.isAutoSleepAllowed === 'boolean') {
      this.conf.globalConfig.isAutoSleepAllowed = req.isAutoSleepAllowed
      return this.conf.globalConfig
    }
    return null
  }

  _start (cb) {
    async.series([
      next => { super._start(next) },
      async () => {
        await this.net_r0.startRpcServer()
        const rpcServer = this.net_r0.rpcServer

        this.racks = await this.store_s1.getBee(
          { name: 'racks' },
          { keyEncoding: 'utf-8' }
        )

        await this.racks.ready()

        this.actionDb = await this.store_s1.getBee(
          { name: 'action-approver' }
        )

        // store tail-log aggr data in db
        this.tailLogAggrDb = await this.store_s1.getBee(
          { name: 'tail-log-aggr' },
          { keyEncoding: 'utf-8' }
        )
        await this.tailLogAggrDb.ready()

        const actionCaller = new ActionCaller(this.net_r0, this.racks, this.conf.ork.callTargetsLimit)
        const actionCallerProxy = new Proxy(actionCaller, {
          get: (target, property, receiver) => {
            // proxy action calls as methods don't exist
            if (typeof target[property] === 'undefined') {
              return (...payload) => {
                const [params, racks] = payload // [params[], targets, requiredPerms]

                return target.callTargets(property, params, racks)
              }
            }
            return Reflect.get(target, property, receiver)
          }
        })
        this.actionCaller = actionCallerProxy
        await this.actionApprover_0.initDb(this.actionDb)
        this.actionApprover_0.initWrk(this.actionCaller)
        await this.actionApprover_0.startInterval(this.conf.ork.actionIntvlMs || 30000)

        // check optional configs loaded
        if (this.conf.aggrData && this.conf.aggrStats) {
        // set interval to store aggregated tailLog data
          this.interval_0.add(
            'aggregate-tail-log',
            this.aggregateTailLogs.bind(this),
            this.conf.aggrData.agrrTailLogIntvlMs || 3600000
          )
        }

        if (this.conf.crossAggrAction) {
          // set interval to add automated cross thing actions
          this.interval_0.add(
            'cross-actions-automation',
            this.crossAggrActions.bind(this),
            this.conf.crossAggrAction.agrrListThingsIntvlMs || 10000
          )
        }

        rpcServer.respond('echo', x => x)

        RPC_METHODS.forEach(method => {
          rpcServer.respond(method.name, async (req, ctx) => {
            try {
              // block write operations for read-only peers
              const remoteKey = ctx._mux.stream.remotePublicKey.toString('hex')
              if (method.op === 'w' && this.net_r0.conf.allowReadOnly?.includes(remoteKey)) {
                throw new Error('ERR_MISSING_WRITE_PERMISSIONS')
              }

              return await this.net_r0.handleReply(method.name, req)
            } catch (err) {
              this.debugError(`rpc ${method.name} failed`, err, true)
              throw err
            }
          })
        })

        this.status.rpcPublicKey = rpcServer.publicKey.toString('hex')

        // rpc client key to be allowed through destination server firewall
        this.status.rpcClientKey = this.net_r0.dht.defaultKeyPair.publicKey.toString('hex')
        this.saveStatus()
      }
    ], cb)
  }
}

module.exports = WrkProcAggr
