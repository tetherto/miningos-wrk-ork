'use strict'

const test = require('brittle')
const WrkProcAggr = require('../../workers/aggr.proc.ork.wrk')
const { RPC_METHODS } = require('../../workers/lib/constants')

// Mock TetherWrkBase dependencies
class MockTetherWrkBase {
  constructor (conf, ctx) {
    this.conf = conf || {}
    this.ctx = ctx || {}
    this.wtype = 'wrk-ork-proc-aggr'
    this.status = {}
    this.mem = {}
    this.confPath = process.cwd() // Set a valid path to avoid errors
    this.facilities = {} // Mock facilities object
  }

  init () {
    // Mock init
  }

  start () {
    // Mock start
  }

  loadConf (name, key) {
    // Mock loadConf - just set the config directly
    if (name === 'base.ork') {
      this.conf.ork = this.conf.ork || { callTargetsLimit: 50, actionIntvlMs: 30000 }
    } else if (key) {
      // For optional configs, just set empty object if not provided
      if (!this.conf[key]) {
        this.conf[key] = {}
      }
    }
  }

  saveStatus () {
    // Mock saveStatus
  }

  setInitFacs (facs) {
    // Mock setInitFacs
  }

  debugError (data, e, alert) {
    // Mock debugError
  }
}

// Mock facilities
class MockStore {
  async getBee (opts, config) {
    const data = new Map()
    return {
      get: async (key) => {
        const value = data.get(key)
        return value ? { value: Buffer.from(JSON.stringify(value)) } : null
      },
      put: async (key, value) => {
        data.set(key, JSON.parse(value.toString()))
      },
      del: async (key) => {
        data.delete(key)
      },
      createReadStream: () => {
        const entries = Array.from(data.entries()).map(([key, value]) => ({
          key,
          value: Buffer.from(JSON.stringify(value))
        }))
        return entries
      },
      ready: () => Promise.resolve()
    }
  }
}

class MockNet {
  constructor () {
    this.handlers = {}
    this.rpcServer = {
      respond: (method, handler) => {
        this.handlers = this.handlers || {}
        this.handlers[method] = handler
      },
      publicKey: {
        toString: () => 'mock-public-key-hex'
      }
    }
    this.dht = {
      defaultKeyPair: {
        publicKey: {
          toString: () => 'mock-client-key-hex'
        }
      }
    }
    this.conf = {}
    this.jRequestCalls = []
  }

  async startRpcServer () {
    // Mock RPC server start
  }

  async jRequest (publicKey, method, params, opts) {
    this.jRequestCalls.push({ publicKey, method, params, opts })
    // Return mock response based on method
    if (method === 'listThings') {
      return [{ id: 'thing1', tags: ['t-miner'] }]
    }
    if (method === 'getHistoricalLogs') {
      return [{ id: 'log1', ts: Date.now() }]
    }
    if (method === 'forgetThings') {
      return 1
    }
    if (method === 'tailLog') {
      return [{ ts: Date.now(), value: 100 }]
    }
    if (method === 'getWrkExtData') {
      return [{ ts: Date.now(), data: 'test' }]
    }
    if (method === 'getWrkConf') {
      return { setting: 'value' }
    }
    if (method === 'getThingConf') {
      return { config: 'value' }
    }
    if (method === 'getWrkSettings') {
      return { setting1: 'value1' }
    }
    if (method === 'saveWrkSettings') {
      return 1
    }
    if (method === 'saveThingComment' || method === 'editThingComment' || method === 'deleteThingComment') {
      return 1
    }
    return null
  }

  handleReply (method, req) {
    // Mock handleReply - delegate to worker method
    return this.worker[method](req)
  }
}

class MockActionApprover {
  constructor () {
    this.actions = new Map()
    this.actionIdCounter = 0
  }

  async initDb (db) {
    // Mock initDb
  }

  initWrk (actionCaller) {
    this.actionCaller = actionCaller
  }

  async startInterval (ms) {
    // Mock startInterval
  }

  async pushAction (opts) {
    const id = `action-${++this.actionIdCounter}`
    const action = {
      id,
      action: opts.action,
      payload: opts.payload,
      voter: opts.voter,
      reqVotesPos: opts.reqVotesPos,
      reqVotesNeg: opts.reqVotesNeg,
      batchActionUID: opts.batchActionUID,
      status: 'voting',
      votes: []
    }
    this.actions.set(`voting-${id}`, action)
    return { id, data: action }
  }

  async getAction (type, id) {
    const key = `${type}-${id}`
    const action = this.actions.get(key)
    if (!action) {
      throw new Error('ERR_ACTION_NOT_FOUND')
    }
    return { data: action }
  }

  async voteAction (opts) {
    const action = this.actions.get(`voting-${opts.id}`)
    if (action) {
      action.votes.push({ voter: opts.voter, approve: opts.approve })
    }
  }

  async cancelActionsBatch (opts) {
    return opts.ids.length
  }

  isActionValid (key, type, filter, action) {
    if (!key.startsWith(`${type}-`)) return false
    const ts = action.timestamp || Date.now()
    return (
      (filter.gte && ts < filter.gte) ||
        (filter.lte && ts > filter.lte) ||
        (filter.gt && ts <= filter.gt) ||
        (filter.lt && ts >= filter.lt)
    )
  }

  query (type, filter, opts) {
    const results = []
    for (const [key, action] of this.actions.entries()) {
      if (this.isActionValid(key, type, filter, action)) {
        results.push(action)
      }
    }
    return results
  }
}

class MockInterval {
  add (name, fn, ms) {
    this.intervals = this.intervals || {}
    this.intervals[name] = { fn, ms }
  }
}

// Helper to create a worker instance with mocks
async function createWorker (conf = {}, ctx = {}) {
  const mockConf = {
    ork: { callTargetsLimit: 50, actionIntvlMs: 30000 },
    ...conf
  }
  const mockCtx = {
    cluster: 'test-cluster',
    ...ctx
  }

  // Create mocks first
  const mockStore = new MockStore()
  const mockNet = new MockNet()
  const mockActionApprover = new MockActionApprover()
  const mockInterval = new MockInterval()

  // Replace TetherWrkBase temporarily to prevent actual initialization
  const basePath = require.resolve('tether-wrk-base/workers/base.wrk.tether')
  const cached = require.cache[basePath]

  require.cache[basePath] = {
    exports: MockTetherWrkBase
  }

  let worker
  try {
    // Temporarily override methods to prevent initialization during construction
    const WrkProcAggrProto = WrkProcAggr.prototype
    const originalStart = WrkProcAggrProto.start
    const originalInit = WrkProcAggrProto.init
    const originalLoadConf = WrkProcAggrProto.loadConf
    const originalLoadOptionalConfigs = WrkProcAggrProto._loadOptionalConfigs
    const originalSaveStatus = WrkProcAggrProto.saveStatus

    // Override loadConf to use mock implementation
    WrkProcAggrProto.loadConf = function (name, key) {
      if (name === 'base.ork') {
        this.conf.ork = this.conf.ork || { callTargetsLimit: 50, actionIntvlMs: 30000 }
      } else if (key) {
        // For optional configs, just set empty object if not provided
        if (!this.conf[key]) {
          this.conf[key] = {}
        }
      }
    }

    // Override _loadOptionalConfigs to prevent errors
    WrkProcAggrProto._loadOptionalConfigs = function () {
      // Mock implementation - do nothing
    }

    // Override init to prevent status directory creation
    WrkProcAggrProto.init = function () {
      // Mock init - do nothing, skip calling super.init()
    }

    // Override saveStatus to prevent file system operations
    WrkProcAggrProto.saveStatus = function () {
      // Mock saveStatus - do nothing
    }

    // Override start to prevent initialization during construction
    WrkProcAggrProto.start = function () {
      // Prevent start during construction - we'll call _start manually in tests
    }

    worker = new WrkProcAggr(mockConf, mockCtx)

    // Restore original methods
    WrkProcAggrProto.start = originalStart
    WrkProcAggrProto.init = originalInit
    WrkProcAggrProto.loadConf = originalLoadConf
    WrkProcAggrProto._loadOptionalConfigs = originalLoadOptionalConfigs
    WrkProcAggrProto.saveStatus = originalSaveStatus

    // Inject mocks
    worker.store_s1 = mockStore
    worker.net_r0 = mockNet
    worker.net_r0.worker = worker
    worker.actionApprover_0 = mockActionApprover
    worker.interval_0 = mockInterval

    // Set wtype if not set by base class (needed for prefix)
    if (!worker.wtype) {
      worker.wtype = 'wrk-ork-proc-aggr'
    }
    // Recalculate prefix with correct wtype
    if (worker.ctx?.cluster) {
      worker.prefix = `${worker.wtype}-${worker.ctx.cluster}`
    }

    // Initialize racks before _start is called
    worker.racks = await mockStore.getBee({ name: 'racks' }, { keyEncoding: 'utf-8' })

    // Initialize actionDb and tailLogAggrDb
    worker.actionDb = await mockStore.getBee({ name: 'action-approver' })
    worker.tailLogAggrDb = await mockStore.getBee({ name: 'tail-log-aggr' }, { keyEncoding: 'utf-8' })

    // Mock _start method
    worker._start = function (cb) {
      // Mock _start - initialize RPC server handlers
      const self = this
      if (self.net_r0) {
        // Register RPC methods
        const { RPC_METHODS } = require('../../workers/lib/constants')
        RPC_METHODS.forEach(method => {
          self.net_r0.rpcServer.respond(method.name, async (req, ctx) => {
            try {
              return await self.net_r0.handleReply(method.name, req)
            } catch (err) {
              self.debugError(`rpc ${method.name} failed`, err, true)
              throw err
            }
          })
        })
      }
      if (self.actionApprover_0?.initDb) {
        self.actionApprover_0.initDb(self.actionDb)
      }
      if (cb) cb()
    }
  } finally {
    // Restore original base class
    if (cached) {
      require.cache[basePath] = cached
    } else {
      delete require.cache[basePath]
    }
  }

  return worker
}

test('Worker initialization', async (t) => {
  t.test('should initialize with valid cluster', async (t) => {
    const worker = await createWorker()
    t.ok(worker, 'should create worker instance')
    t.is(worker.ctx.cluster, 'test-cluster', 'should set cluster')
    t.is(worker.prefix, 'wrk-ork-proc-aggr-test-cluster', 'should set prefix')
  })

  t.test('should throw error without cluster', async (t) => {
    try {
      // eslint-disable-next-line no-new
      new WrkProcAggr({}, {})
      t.fail('should throw error')
    } catch (err) {
      t.is(err.message, 'ERR_PROC_RACK_UNDEFINED', 'should throw correct error')
    }
  })
})

test('registerRack', async (t) => {
  t.test('should register a rack successfully', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    const req = {
      id: 'rack-1',
      type: 'wrk-miner-s19',
      info: {
        rpcPublicKey: 'test-public-key'
      }
    }

    const result = await worker.registerRack(req)
    t.is(result, 1, 'should return 1 on success')

    const rack = await worker.racks.get('rack-1')
    t.ok(rack, 'should store rack')
    const rackData = JSON.parse(rack.value.toString())
    t.is(rackData.id, 'rack-1', 'should store correct id')
    t.is(rackData.type, 'wrk-miner-s19', 'should store correct type')
  })

  t.test('should throw error for missing id', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    try {
      await worker.registerRack({ type: 'wrk-miner-s19', info: { rpcPublicKey: 'key' } })
      t.fail('should throw error')
    } catch (err) {
      t.is(err.message, 'ERR_RACK_ID_INVALID', 'should throw correct error')
    }
  })

  t.test('should throw error for missing type', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    try {
      await worker.registerRack({ id: 'rack-1', info: { rpcPublicKey: 'key' } })
      t.fail('should throw error')
    } catch (err) {
      t.is(err.message, 'ERR_RACK_TYPE_INVALID', 'should throw correct error')
    }
  })

  t.test('should throw error for missing rpcPublicKey', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    try {
      await worker.registerRack({ id: 'rack-1', type: 'wrk-miner-s19', info: {} })
      t.fail('should throw error')
    } catch (err) {
      t.is(err.message, 'ERR_RACK_INFO_RPC_PUBKEY_INVALID', 'should throw correct error')
    }
  })
})

test('listRacks', async (t) => {
  t.test('should list all racks', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    // Register some racks
    await worker.registerRack({
      id: 'rack-1',
      type: 'wrk-miner-s19',
      info: { rpcPublicKey: 'key1' }
    })
    await worker.registerRack({
      id: 'rack-2',
      type: 'wrk-container',
      info: { rpcPublicKey: 'key2' }
    })

    const result = await worker.listRacks({})
    t.is(result.length, 2, 'should return all racks')
    t.ok(result.find(r => r.id === 'rack-1'), 'should include rack-1')
    t.ok(result.find(r => r.id === 'rack-2'), 'should include rack-2')
  })

  t.test('should filter racks by type', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    await worker.registerRack({
      id: 'rack-1',
      type: 'wrk-miner-s19',
      info: { rpcPublicKey: 'key1' }
    })
    await worker.registerRack({
      id: 'rack-2',
      type: 'wrk-container',
      info: { rpcPublicKey: 'key2' }
    })

    const result = await worker.listRacks({ type: 'wrk-miner' })
    t.is(result.length, 1, 'should return filtered racks')
    t.is(result[0].id, 'rack-1', 'should return correct rack')
  })

  t.test('should not expose rpcPublicKey by default', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    await worker.registerRack({
      id: 'rack-1',
      type: 'wrk-miner-s19',
      info: { rpcPublicKey: 'secret-key' }
    })

    const result = await worker.listRacks({})
    t.is(result.length, 1, 'should return rack')
    t.ok(!result[0].info.rpcPublicKey, 'should not expose rpcPublicKey')
  })

  t.test('should expose rpcPublicKey when keys=true', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    await worker.registerRack({
      id: 'rack-1',
      type: 'wrk-miner-s19',
      info: { rpcPublicKey: 'secret-key' }
    })

    const result = await worker.listRacks({ keys: true })
    t.is(result.length, 1, 'should return rack')
    t.is(result[0].info.rpcPublicKey, 'secret-key', 'should expose rpcPublicKey')
  })

  t.test('should throw error for invalid type', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    try {
      await worker.listRacks({ type: 123 })
      t.fail('should throw error')
    } catch (err) {
      t.is(err.message, 'ERR_TYPE_INVALID', 'should throw correct error')
    }
  })
})

test('forgetRacks', async (t) => {
  t.test('should forget racks by ids', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    await worker.registerRack({
      id: 'rack-1',
      type: 'wrk-miner-s19',
      info: { rpcPublicKey: 'key1' }
    })
    await worker.registerRack({
      id: 'rack-2',
      type: 'wrk-miner-s19',
      info: { rpcPublicKey: 'key2' }
    })

    const result = await worker.forgetRacks({ ids: ['rack-1'] })
    t.is(result, 1, 'should return count of deleted racks')

    const remaining = await worker.listRacks({})
    t.is(remaining.length, 1, 'should have one rack remaining')
    t.is(remaining[0].id, 'rack-2', 'should keep rack-2')
  })

  t.test('should forget all racks when all=true', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    await worker.registerRack({
      id: 'rack-1',
      type: 'wrk-miner-s19',
      info: { rpcPublicKey: 'key1' }
    })
    await worker.registerRack({
      id: 'rack-2',
      type: 'wrk-miner-s19',
      info: { rpcPublicKey: 'key2' }
    })

    const result = await worker.forgetRacks({ all: true })
    t.is(result, 2, 'should return count of all deleted racks')

    const remaining = await worker.listRacks({})
    t.is(remaining.length, 0, 'should have no racks remaining')
  })
})

test('listThings', async (t) => {
  t.test('should list things from all racks', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    await worker.registerRack({
      id: 'rack-1',
      type: 'wrk-miner-s19',
      info: { rpcPublicKey: 'key1' }
    })

    const result = await worker.listThings({})
    t.ok(Array.isArray(result), 'should return array')
    t.is(result.length, 1, 'should return things from racks')
  })

  t.test('should handle errors from racks gracefully', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    worker.net_r0.jRequest = async () => {
      throw new Error('Network error')
    }

    await worker.registerRack({
      id: 'rack-1',
      type: 'wrk-miner-s19',
      info: { rpcPublicKey: 'key1' }
    })

    const result = await worker.listThings({})
    t.ok(Array.isArray(result), 'should return array')
    t.is(result.length, 0, 'should return empty array on error')
  })
})

test('getHistoricalLogs', async (t) => {
  t.test('should get historical logs', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    await worker.registerRack({
      id: 'rack-1',
      type: 'wrk-miner-s19',
      info: { rpcPublicKey: 'key1' }
    })

    const result = await worker.getHistoricalLogs({ logType: 'test' })
    t.ok(Array.isArray(result), 'should return array')
  })

  t.test('should throw error for missing logType', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    try {
      await worker.getHistoricalLogs({})
      t.fail('should throw error')
    } catch (err) {
      t.is(err.message, 'ERR_LOG_TYPE_INVALID', 'should throw correct error')
    }
  })
})

test('forgetThings', async (t) => {
  t.test('should forget things from all racks', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    await worker.registerRack({
      id: 'rack-1',
      type: 'wrk-miner-s19',
      info: { rpcPublicKey: 'key1' }
    })

    const result = await worker.forgetThings({ query: { id: 'thing1' } })
    t.is(typeof result, 'number', 'should return number')
  })
})

test('pushAction', async (t) => {
  t.test('should push action successfully', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    // Mock actionCaller
    worker.actionCaller = {
      getWriteCalls: async () => ({
        targets: {
          'rack-1': {
            reqVotes: 1,
            calls: [{ id: 'thing1' }]
          }
        },
        requiredPerms: ['miner:rw']
      })
    }

    const req = {
      query: { tags: { $in: ['t-miner'] } },
      action: 'reboot',
      params: [],
      voter: 'test@example.com',
      authPerms: ['miner:rw']
    }

    const result = await worker.pushAction(req)
    t.ok(result.id, 'should return action id')
    t.ok(Array.isArray(result.errors), 'should return errors array')
  })

  t.test('should return errors when no calls', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    worker.actionCaller = {
      getWriteCalls: async () => ({
        targets: {
          'rack-1': {
            reqVotes: 1,
            calls: []
          }
        },
        requiredPerms: ['miner:rw']
      })
    }

    const req = {
      query: { tags: { $in: ['t-miner'] } },
      action: 'reboot',
      params: [],
      voter: 'test@example.com',
      authPerms: ['miner:rw']
    }

    const result = await worker.pushAction(req)
    t.ok(result.errors.length > 0, 'should return errors')
  })
})

test('pushActionsBatch', async (t) => {
  t.test('should push batch actions', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    worker.actionCaller = {
      getWriteCalls: async () => ({
        targets: {
          'rack-1': {
            reqVotes: 1,
            calls: [{ id: 'thing1' }]
          }
        },
        requiredPerms: ['miner:rw']
      })
    }

    const req = {
      batchActionsPayload: [
        {
          query: { tags: { $in: ['t-miner'] } },
          action: 'reboot',
          params: []
        }
      ],
      voter: 'test@example.com',
      authPerms: ['miner:rw']
    }

    const result = await worker.pushActionsBatch(req)
    t.ok(Array.isArray(result), 'should return array')
  })

  t.test('should throw error for invalid payload', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    try {
      await worker.pushActionsBatch({ batchActionsPayload: 'invalid' })
      t.fail('should throw error')
    } catch (err) {
      t.is(err.message, 'ERR_PAYLOAD_INVALID', 'should throw correct error')
    }
  })
})

test('getAction', async (t) => {
  t.test('should get action by id', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    // Create an action first
    const action = await worker.actionApprover_0.pushAction({
      action: 'reboot',
      payload: [['param1'], { 'rack-1': { calls: [] } }, ['miner:rw']],
      voter: 'test@example.com',
      reqVotesPos: 1,
      reqVotesNeg: 1
    })

    const result = await worker.getAction({ id: action.id, type: 'voting' })
    t.ok(result, 'should return action')
    t.is(result.action, 'reboot', 'should have correct action')
    t.ok(result.params, 'should have params')
    t.ok(result.targets, 'should have targets')
    t.ok(result.requiredPerms, 'should have requiredPerms')
  })
})

test('voteAction', async (t) => {
  t.test('should vote on action', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    const action = await worker.actionApprover_0.pushAction({
      action: 'reboot',
      payload: [['param1'], { 'rack-1': { calls: [] } }, ['miner:rw']],
      voter: 'test@example.com',
      reqVotesPos: 1,
      reqVotesNeg: 1
    })

    const result = await worker.voteAction({
      id: action.id,
      voter: 'voter@example.com',
      approve: true,
      authPerms: ['miner:rw']
    })

    t.is(result, 1, 'should return 1 on success')
  })

  t.test('should throw error for missing permissions', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    const action = await worker.actionApprover_0.pushAction({
      action: 'reboot',
      payload: [['param1'], { 'rack-1': { calls: [] } }, ['miner:rw']],
      voter: 'test@example.com',
      reqVotesPos: 1,
      reqVotesNeg: 1
    })

    try {
      await worker.voteAction({
        id: action.id,
        voter: 'voter@example.com',
        approve: true,
        authPerms: ['miner:r'] // Missing write permission
      })
      t.fail('should throw error')
    } catch (err) {
      t.is(err.message, 'ERR_ACTION_DENIED', 'should throw correct error')
    }
  })
})

test('queryActions', async (t) => {
  t.test('should query actions', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    await worker.actionApprover_0.pushAction({
      action: 'reboot',
      payload: [['param1'], { 'rack-1': { calls: [] } }, ['miner:rw']],
      voter: 'test@example.com',
      reqVotesPos: 1,
      reqVotesNeg: 1
    })

    const result = await worker.queryActions({
      queries: [
        {
          type: 'voting',
          filter: { gte: 0 }
        }
      ]
    })

    t.ok(result.voting, 'should return voting actions')
    t.ok(Array.isArray(result.voting), 'should return array')
  })

  t.test('should throw error for invalid queries', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    try {
      await worker.queryActions({ queries: 'invalid' })
      t.fail('should throw error')
    } catch (err) {
      t.is(err.message, 'ERR_QUERIES_INVALID', 'should throw correct error')
    }
  })

  t.test('should throw error for invalid query type', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    try {
      await worker.queryActions({
        queries: [{ type: 123 }]
      })
      t.fail('should throw error')
    } catch (err) {
      t.is(err.message, 'ERR_QUERIES_TYPE_INVALID', 'should throw correct error')
    }
  })
})

test('getActionsBatch', async (t) => {
  t.test('should get actions batch', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    const action = await worker.actionApprover_0.pushAction({
      action: 'reboot',
      payload: [['param1'], { 'rack-1': { calls: [] } }, ['miner:rw']],
      voter: 'test@example.com',
      reqVotesPos: 1,
      reqVotesNeg: 1
    })

    const result = await worker.getActionsBatch({ ids: [action.id] })
    t.ok(Array.isArray(result), 'should return array')
  })
})

test('cancelActionsBatch', async (t) => {
  t.test('should cancel actions batch', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    const result = await worker.cancelActionsBatch({
      ids: ['action-1', 'action-2'],
      voter: 'test@example.com'
    })

    t.is(result, 2, 'should return count of cancelled actions')
  })
})

test('getGlobalConfig', async (t) => {
  t.test('should get global config', async (t) => {
    const worker = await createWorker({
      globalConfig: { setting: 'value' }
    })
    worker._start(() => {})

    const result = await worker.getGlobalConfig({})
    t.ok(result, 'should return config')
    t.is(result.setting, 'value', 'should have correct setting')
  })

  t.test('should throw error when config missing', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    try {
      await worker.getGlobalConfig({})
      t.fail('should throw error')
    } catch (err) {
      t.is(err.message, 'ERR_GLOBAL_CONFIG_MISSING', 'should throw correct error')
    }
  })
})

test('setGlobalConfig', async (t) => {
  t.test('should set global config', async (t) => {
    const worker = await createWorker({
      globalConfig: { isAutoSleepAllowed: false }
    })
    worker._start(() => {})

    const result = await worker.setGlobalConfig({ isAutoSleepAllowed: true })
    t.ok(result, 'should return config')
    t.is(result.isAutoSleepAllowed, true, 'should update config')
  })

  t.test('should throw error for invalid config', async (t) => {
    const worker = await createWorker({
      globalConfig: {}
    })
    worker._start(() => {})

    try {
      await worker.setGlobalConfig({})
      t.fail('should throw error')
    } catch (err) {
      t.is(err.message, 'ERR_CONFIG_INVALID', 'should throw correct error')
    }
  })

  t.test('should throw error when global config not found', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    try {
      await worker.setGlobalConfig({ isAutoSleepAllowed: true })
      t.fail('should throw error')
    } catch (err) {
      t.is(err.message, 'ERR_GLOBAL_CONFIG_NOT_FOUND', 'should throw correct error')
    }
  })
})

test('getWrkConf', async (t) => {
  t.test('should get worker config', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    await worker.registerRack({
      id: 'rack-1',
      type: 'wrk-miner-s19',
      info: { rpcPublicKey: 'key1' }
    })

    const result = await worker.getWrkConf({ type: 'wrk-miner-s19' })
    t.ok(Array.isArray(result), 'should return array')
  })

  t.test('should throw error for missing type', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    try {
      await worker.getWrkConf({})
      t.fail('should throw error')
    } catch (err) {
      t.is(err.message, 'ERR_TYPE_INVALID', 'should throw correct error')
    }
  })
})

test('getThingConf', async (t) => {
  t.test('should get thing config', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    await worker.registerRack({
      id: 'rack-1',
      type: 'wrk-miner-s19',
      info: { rpcPublicKey: 'key1' }
    })

    const result = await worker.getThingConf({ type: 'wrk-miner-s19' })
    t.ok(Array.isArray(result), 'should return array')
  })

  t.test('should throw error for missing type', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    try {
      await worker.getThingConf({})
      t.fail('should throw error')
    } catch (err) {
      t.is(err.message, 'ERR_TYPE_INVALID', 'should throw correct error')
    }
  })
})

test('getWrkSettings', async (t) => {
  t.test('should get worker settings', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    await worker.registerRack({
      id: 'rack-1',
      type: 'wrk-miner-s19',
      info: { rpcPublicKey: 'key1' }
    })

    const result = await worker.getWrkSettings({ rackId: 'rack-1' })
    t.ok(result, 'should return settings')
  })

  t.test('should throw error for invalid rackId', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    try {
      await worker.getWrkSettings({ rackId: 123 })
      t.fail('should throw error')
    } catch (err) {
      t.is(err.message, 'ERR_RACK_ID_INVALID', 'should throw correct error')
    }
  })
})

test('saveWrkSettings', async (t) => {
  t.test('should save worker settings', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    await worker.registerRack({
      id: 'rack-1',
      type: 'wrk-miner-s19',
      info: { rpcPublicKey: 'key1' }
    })

    const result = await worker.saveWrkSettings({
      rackId: 'rack-1',
      entries: { setting1: 'value1' }
    })
    t.is(result, 1, 'should return 1 on success')
  })

  t.test('should throw error for invalid rackId', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    try {
      await worker.saveWrkSettings({ rackId: 123, entries: {} })
      t.fail('should throw error')
    } catch (err) {
      t.is(err.message, 'ERR_RACK_ID_INVALID', 'should throw correct error')
    }
  })

  t.test('should throw error for invalid entries', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    try {
      await worker.saveWrkSettings({ rackId: 'rack-1', entries: 'invalid' })
      t.fail('should throw error')
    } catch (err) {
      t.is(err.message, 'ERR_ENTRIES_INVALID', 'should throw correct error')
    }
  })
})

test('saveThingComment', async (t) => {
  t.test('should save thing comment', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    await worker.registerRack({
      id: 'rack-1',
      type: 'wrk-miner-s19',
      info: { rpcPublicKey: 'key1' }
    })

    const result = await worker.saveThingComment({
      rackId: 'rack-1',
      thingId: 'thing-1',
      comment: 'test comment'
    })
    t.is(result, 1, 'should return 1 on success')
  })

  t.test('should throw error for missing rackId', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    try {
      await worker.saveThingComment({ thingId: 'thing-1' })
      t.fail('should throw error')
    } catch (err) {
      t.is(err.message, 'ERR_RACK_ID_INVALID', 'should throw correct error')
    }
  })

  t.test('should throw error for missing thingId', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    try {
      await worker.saveThingComment({ rackId: 'rack-1' })
      t.fail('should throw error')
    } catch (err) {
      t.is(err.message, 'ERR_THING_ID_INVALID', 'should throw correct error')
    }
  })
})

test('editThingComment', async (t) => {
  t.test('should edit thing comment', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    await worker.registerRack({
      id: 'rack-1',
      type: 'wrk-miner-s19',
      info: { rpcPublicKey: 'key1' }
    })

    const result = await worker.editThingComment({
      rackId: 'rack-1',
      thingId: 'thing-1',
      comment: 'updated comment'
    })
    t.is(result, 1, 'should return 1 on success')
  })
})

test('deleteThingComment', async (t) => {
  t.test('should delete thing comment', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    await worker.registerRack({
      id: 'rack-1',
      type: 'wrk-miner-s19',
      info: { rpcPublicKey: 'key1' }
    })

    const result = await worker.deleteThingComment({
      rackId: 'rack-1',
      thingId: 'thing-1'
    })
    t.is(result, 1, 'should return 1 on success')
  })
})

test('tailLog', async (t) => {
  t.test('should throw error when not configured', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    try {
      await worker.tailLog({ type: 'miner' })
      t.fail('should throw error')
    } catch (err) {
      t.is(err.message, 'ERR_NOT_CONFIGURED', 'should throw correct error')
    }
  })

  t.test('should throw error for missing type', async (t) => {
    const worker = await createWorker({
      aggrStats: { miner: { ops: {} } }
    })
    worker._start(() => {})

    try {
      await worker.tailLog({})
      t.fail('should throw error')
    } catch (err) {
      t.is(err.message, 'ERR_TYPE_INVALID', 'should throw correct error')
    }
  })
})

test('tailLogMulti', async (t) => {
  t.test('should throw error for invalid keys', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    try {
      await worker.tailLogMulti({ keys: 'invalid' })
      t.fail('should throw error')
    } catch (err) {
      t.is(err.message, 'ERR_KEYS_INVALID', 'should throw correct error')
    }
  })

  t.test('should throw error for missing type in keys', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    try {
      await worker.tailLogMulti({ keys: [{ key: 'test' }] })
      t.fail('should throw error')
    } catch (err) {
      t.is(err.message, 'ERR_TYPE_INVALID', 'should throw correct error')
    }
  })
})

test('tailLogCustomRangeAggr', async (t) => {
  t.test('should throw error when not configured', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    try {
      await worker.tailLogCustomRangeAggr({ keys: [] })
      t.fail('should throw error')
    } catch (err) {
      t.is(err.message, 'ERR_NOT_CONFIGURED', 'should throw correct error')
    }
  })

  t.test('should throw error for invalid keys', async (t) => {
    const worker = await createWorker({
      aggrData: {},
      aggrStats: {},
      globalConfig: { aggrTailLogTimezones: [{ code: 'UTC', offset: 0 }] }
    })
    worker._start(() => {})

    try {
      await worker.tailLogCustomRangeAggr({ keys: 'invalid' })
      t.fail('should throw error')
    } catch (err) {
      t.is(err.message, 'ERR_KEYS_INVALID', 'should throw correct error')
    }
  })
})

test('getWrkExtData', async (t) => {
  t.test('should get worker ext data', async (t) => {
    const worker = await createWorker({
      aggrStats: { miner: { ops: {} } }
    })
    worker._start(() => {})

    await worker.registerRack({
      id: 'rack-1',
      type: 'wrk-miner-s19',
      info: { rpcPublicKey: 'key1' }
    })

    const result = await worker.getWrkExtData({ type: 'wrk-miner-s19' })
    t.ok(Array.isArray(result), 'should return array')
  })

  t.test('should throw error for missing type', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    try {
      await worker.getWrkExtData({})
      t.fail('should throw error')
    } catch (err) {
      t.is(err.message, 'ERR_TYPE_INVALID', 'should throw correct error')
    }
  })
})

test('RPC method registration', async (t) => {
  t.test('should register all RPC methods', async (t) => {
    const worker = await createWorker()
    worker._start(() => {})

    t.ok(worker.net_r0.handlers, 'should have handlers')
    for (const method of RPC_METHODS) {
      t.ok(worker.net_r0.handlers[method.name], `should register ${method.name}`)
    }
  })
})
