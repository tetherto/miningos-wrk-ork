'use strict'

const async = require('async')
const NetFacility = require('hp-svc-facs-net')
const Hyperbee = require('hyperbee')
const mingo = require('mingo')
const { isPlainObject } = require('@bitfinex/lib-js-util-base')
const { ACTION_TYPES } = require('./constants')
const { hasWritePermission } = require('./permissions')

class ActionCaller {
  /**
   * @param {NetFacility} net
   * @param {Hyperbee} racks
   */
  constructor (net, racks, callTargetsLimit = 50) {
    if (!(net instanceof NetFacility)) {
      throw new Error('ERR_NET_INVALID_INSTANCE')
    }

    if (!(racks instanceof Hyperbee)) {
      throw new Error('ERR_RACKS_INSTANCE_INVALID')
    }

    this._net = net
    this._racks = racks
    this.rackActions = new Set([ACTION_TYPES.REGISTER_THING, ACTION_TYPES.UPDATE_THING, ACTION_TYPES.FORGET_THINGS, ACTION_TYPES.RACK_REBOOT])
    this._callTargetsLimit = callTargetsLimit
  }

  /**
   * @param {string} rack
   * @param {string} id
   * @param {string} method
   * @param {any[]} params
   */
  async _callThing (rack, id, method, params) {
    const raw = await this._racks.get(rack)
    const entry = JSON.parse(raw.value.toString())

    if (this.rackActions.has(method)) {
      let formattedParams = {}
      let query
      switch (method) {
        case ACTION_TYPES.REGISTER_THING:
        case ACTION_TYPES.UPDATE_THING: {
          formattedParams = params[0]
          const paramOpts = params[params.length - 1]
          formattedParams.actionId = paramOpts.actionId
          formattedParams.user = paramOpts.user
          break
        }

        case ACTION_TYPES.FORGET_THINGS: {
          query = params[0]?.query
          const paramOpts = params[params.length - 1]
          const actionId = paramOpts.actionId
          formattedParams = { query, actionId }
          break
        }
      }
      return this._net.jRequest(
        entry.info.rpcPublicKey,
        method,
        formattedParams
      )
    }

    return this._net.jRequest(
      entry.info.rpcPublicKey,
      'queryThing',
      { id, method, params }
    )
  }

  /**
   * @param {object} query
   * @param {string} action
   * @param {any[]} params
   * @param {string[]} rackTypes
   * @returns {Promise<{
    *  requiredPerms: string[],
    *  targets: Object<
    *    string, {
    *      reqVotes: number,
    *      calls: Array<{id: string, tags: string[]}>,
    *      error?: string
    *    }>
   *  }>}
   */
  async getWriteCalls (query, action, params, permissions) {
    if (!isPlainObject(query)) {
      throw new Error('ERR_QUERY_INVALID')
    }

    try {
      // invalid mongo query throws error
      const mingoQuery = new mingo.Query(query)
      if (!mingoQuery) throw new Error('ERR_QUERY_INVALID')
    } catch (e) { throw new Error('ERR_QUERY_INVALID') }

    if (!action || typeof action !== 'string') {
      throw new Error('ERR_ACTION_INVALID')
    }
    if (!Array.isArray(params)) {
      throw new Error('ERR_PARAMS_INVALID')
    }

    const targets = {}
    const stream = this._racks.createReadStream()
    const limit = 5
    const requiredPerms = new Set()

    await async.eachLimit(stream, limit, async (raw) => {
      const entry = JSON.parse(raw.value.toString())
      const baseType = entry.type.split('-')[0]

      if (!hasWritePermission(permissions, baseType)) {
        return
      }

      try {
        let rackActionId
        if (this.rackActions.has(action)) {
          this._validateRackAction(action, params)

          // if not reboot action then only add actions for rackId in params
          if (action !== ACTION_TYPES.RACK_REBOOT && params[0].rackId !== entry.id) return

          rackActionId = params[0]?.id || params[0]?.query?.id || entry.id
        }

        const res = await this._net.jRequest(
          entry.info.rpcPublicKey,
          'getWriteCalls',
          { query, action, params, rackActionId },
          { timeout: 120000 }
        )

        if (res.calls.length) {
          targets[entry.id] = { reqVotes: res.reqVotes, calls: res.calls }
          requiredPerms.add(baseType)
        }
      } catch (err) {
        console.error(`getWriteCalls failed for ${entry.id}`, err)
        targets[entry.id] = { reqVotes: 1, calls: [], error: err.message }
      }
    })

    return { targets, requiredPerms: Array.from(requiredPerms) }
  }

  /**
   * @param {string} action
   * @param {any[]} params
   */
  _validateRackAction (action, params) {
    if (action === ACTION_TYPES.RACK_REBOOT) {
      return
    }
    if (!params[0]?.rackId) {
      throw new Error('ERR_ACTION_INVALID_MISSING_RACKID')
    }

    if (action === ACTION_TYPES.UPDATE_THING && !params[0]?.id) {
      throw new Error('ERR_ACTION_INVALID_MISSING_ID')
    }

    if (action === ACTION_TYPES.FORGET_THINGS && typeof params[0]?.query?.id !== 'string') {
      throw new Error('ERR_ACTION_INVALID_QUERY_ID')
    }
  }

  /**
   * @param {string} action
   * @param {any[]} params
   * @param {Object<string, { calls: Array<{id: string, tags: string[]}>, error?: string }>} targets
   */
  async callTargets (action, params, targets) {
    const calls = Object.entries(targets).map(
      ([rack, entry]) => entry.calls.map(call => [rack, call])
    ).flat(1)

    await async.eachLimit(calls, this._callTargetsLimit, async ([rack, call]) => {
      try {
        const result = await this._callThing(rack, call.id, action, params)
        call.result = result
      } catch (err) {
        call.error = err.message
      }
    })
  }
}

module.exports = ActionCaller
