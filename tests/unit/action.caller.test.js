'use strict'

const test = require('brittle')
const ActionCaller = require('../../workers/lib/action.caller')
const { ACTION_TYPES } = require('../../workers/lib/constants')

// Mock NetFacility - we'll bypass instanceof checks in tests
class MockNetFacility {
  constructor () {
    this.jRequestCalls = []
  }

  async jRequest (publicKey, method, params, opts) {
    this.jRequestCalls.push({ publicKey, method, params, opts })
    return { calls: [], reqVotes: 1 }
  }
}

// Mock Hyperbee - we'll bypass instanceof checks in tests
class MockHyperbee {
  constructor (data = {}) {
    this.data = data
  }

  async get (key) {
    if (this.data[key]) {
      return { value: Buffer.from(JSON.stringify(this.data[key])) }
    }
    return null
  }

  createReadStream () {
    const entries = Object.entries(this.data).map(([key, value]) => ({
      key,
      value: Buffer.from(JSON.stringify(value))
    }))
    return entries
  }
}

// Helper to create ActionCaller bypassing instanceof checks
function createActionCaller (net, racks, callTargetsLimit) {
  const caller = Object.create(ActionCaller.prototype)
  caller._net = net
  caller._racks = racks
  caller.rackActions = new Set([ACTION_TYPES.REGISTER_THING, ACTION_TYPES.UPDATE_THING, ACTION_TYPES.FORGET_THINGS, ACTION_TYPES.RACK_REBOOT])
  caller._callTargetsLimit = callTargetsLimit || 50
  return caller
}

test('ActionCaller constructor', async (t) => {
  t.test('should create instance with valid parameters', async (t) => {
    const net = new MockNetFacility()
    const racks = new MockHyperbee()
    const caller = createActionCaller(net, racks)
    t.ok(caller, 'should create instance')
    t.is(caller._net, net, 'should store net')
    t.is(caller._racks, racks, 'should store racks')
  })

  t.test('should accept callTargetsLimit parameter', async (t) => {
    const net = new MockNetFacility()
    const racks = new MockHyperbee()
    const caller = createActionCaller(net, racks, 100)
    t.is(caller._callTargetsLimit, 100, 'should set callTargetsLimit')
  })

  t.test('should have rackActions set', async (t) => {
    const net = new MockNetFacility()
    const racks = new MockHyperbee()
    const caller = createActionCaller(net, racks)
    t.ok(caller.rackActions.has(ACTION_TYPES.REGISTER_THING), 'should include REGISTER_THING')
    t.ok(caller.rackActions.has(ACTION_TYPES.UPDATE_THING), 'should include UPDATE_THING')
    t.ok(caller.rackActions.has(ACTION_TYPES.FORGET_THINGS), 'should include FORGET_THINGS')
    t.ok(caller.rackActions.has(ACTION_TYPES.RACK_REBOOT), 'should include RACK_REBOOT')
  })
})

test('ActionCaller getWriteCalls', async (t) => {
  t.test('should throw error for invalid query', async (t) => {
    const net = new MockNetFacility()
    const racks = new MockHyperbee()
    const caller = createActionCaller(net, racks)

    try {
      await caller.getWriteCalls(null, 'action', [], [])
      t.fail('should throw error')
    } catch (err) {
      t.is(err.message, 'ERR_QUERY_INVALID', 'should throw correct error')
    }

    try {
      await caller.getWriteCalls('invalid', 'action', [], [])
      t.fail('should throw error')
    } catch (err) {
      t.is(err.message, 'ERR_QUERY_INVALID', 'should throw correct error')
    }
  })

  t.test('should throw error for invalid action', async (t) => {
    const net = new MockNetFacility()
    const racks = new MockHyperbee()
    const caller = createActionCaller(net, racks)

    try {
      await caller.getWriteCalls({}, null, [], [])
      t.fail('should throw error')
    } catch (err) {
      t.is(err.message, 'ERR_ACTION_INVALID', 'should throw correct error')
    }

    try {
      await caller.getWriteCalls({}, 123, [], [])
      t.fail('should throw error')
    } catch (err) {
      t.is(err.message, 'ERR_ACTION_INVALID', 'should throw correct error')
    }
  })

  t.test('should throw error for invalid params', async (t) => {
    const net = new MockNetFacility()
    const racks = new MockHyperbee()
    const caller = createActionCaller(net, racks)

    try {
      await caller.getWriteCalls({}, 'action', null, [])
      t.fail('should throw error')
    } catch (err) {
      t.is(err.message, 'ERR_PARAMS_INVALID', 'should throw correct error')
    }
  })

  t.test('should return empty targets for empty racks', async (t) => {
    const net = new MockNetFacility()
    const racks = new MockHyperbee()
    const caller = createActionCaller(net, racks)

    const result = await caller.getWriteCalls({}, 'action', [], [])
    t.ok(result.targets, 'should have targets')
    t.ok(Array.isArray(result.requiredPerms), 'should have requiredPerms array')
    t.is(Object.keys(result.targets).length, 0, 'should have no targets')
  })

  t.test('should handle invalid mongo query', async (t) => {
    const net = new MockNetFacility()
    const racks = new MockHyperbee()
    const caller = createActionCaller(net, racks)

    try {
      await caller.getWriteCalls({ $invalid: 'query' }, 'action', [], [])
      t.fail('should throw error')
    } catch (err) {
      t.is(err.message, 'ERR_QUERY_INVALID', 'should throw correct error')
    }
  })
})

test('ActionCaller _validateRackAction', async (t) => {
  t.test('should not throw for RACK_REBOOT', async (t) => {
    const net = new MockNetFacility()
    const racks = new MockHyperbee()
    const caller = createActionCaller(net, racks)

    try {
      caller._validateRackAction(ACTION_TYPES.RACK_REBOOT, [])
      t.pass('should not throw')
    } catch (err) {
      t.fail('should not throw error')
    }
  })

  t.test('should throw for missing rackId', async (t) => {
    const net = new MockNetFacility()
    const racks = new MockHyperbee()
    const caller = createActionCaller(net, racks)

    try {
      caller._validateRackAction(ACTION_TYPES.REGISTER_THING, [{}])
      t.fail('should throw error')
    } catch (err) {
      t.is(err.message, 'ERR_ACTION_INVALID_MISSING_RACKID', 'should throw correct error')
    }
  })

  t.test('should throw for UPDATE_THING missing id', async (t) => {
    const net = new MockNetFacility()
    const racks = new MockHyperbee()
    const caller = createActionCaller(net, racks)

    try {
      caller._validateRackAction(ACTION_TYPES.UPDATE_THING, [{ rackId: 'rack1' }])
      t.fail('should throw error')
    } catch (err) {
      t.is(err.message, 'ERR_ACTION_INVALID_MISSING_ID', 'should throw correct error')
    }
  })

  t.test('should throw for FORGET_THINGS missing query.id', async (t) => {
    const net = new MockNetFacility()
    const racks = new MockHyperbee()
    const caller = createActionCaller(net, racks)

    try {
      caller._validateRackAction(ACTION_TYPES.FORGET_THINGS, [{ rackId: 'rack1', query: {} }])
      t.fail('should throw error')
    } catch (err) {
      t.is(err.message, 'ERR_ACTION_INVALID_QUERY_ID', 'should throw correct error')
    }
  })

  t.test('should not throw for valid UPDATE_THING', async (t) => {
    const net = new MockNetFacility()
    const racks = new MockHyperbee()
    const caller = createActionCaller(net, racks)

    try {
      caller._validateRackAction(ACTION_TYPES.UPDATE_THING, [{ rackId: 'rack1', id: 'thing1' }])
      t.pass('should not throw')
    } catch (err) {
      t.fail('should not throw error')
    }
  })

  t.test('should not throw for valid FORGET_THINGS', async (t) => {
    const net = new MockNetFacility()
    const racks = new MockHyperbee()
    const caller = createActionCaller(net, racks)

    try {
      caller._validateRackAction(ACTION_TYPES.FORGET_THINGS, [{ rackId: 'rack1', query: { id: 'thing1' } }])
      t.pass('should not throw')
    } catch (err) {
      t.fail('should not throw error')
    }
  })
})

test('ActionCaller callTargets', async (t) => {
  t.test('should call targets with correct parameters', async (t) => {
    const net = new MockNetFacility()
    const racks = new MockHyperbee({
      rack1: {
        id: 'rack1',
        info: { rpcPublicKey: 'key1' }
      }
    })
    const caller = createActionCaller(net, racks)

    const targets = {
      rack1: {
        calls: [
          { id: 'thing1', tags: ['tag1'] },
          { id: 'thing2', tags: ['tag2'] }
        ]
      }
    }

    await caller.callTargets('action', ['param1', 'param2'], targets)

    t.is(net.jRequestCalls.length, 2, 'should make 2 calls')
    t.is(net.jRequestCalls[0].method, 'queryThing', 'should use queryThing method')
    t.is(net.jRequestCalls[0].params.id, 'thing1', 'should pass correct thing id')
    t.is(net.jRequestCalls[0].params.method, 'action', 'should pass correct action')
  })

  t.test('should handle errors in calls', async (t) => {
    const net = new MockNetFacility()
    net.jRequest = async () => {
      throw new Error('Network error')
    }
    const racks = new MockHyperbee({
      rack1: {
        id: 'rack1',
        info: { rpcPublicKey: 'key1' }
      }
    })
    const caller = createActionCaller(net, racks)

    const targets = {
      rack1: {
        calls: [{ id: 'thing1', tags: ['tag1'] }]
      }
    }

    await caller.callTargets('action', ['param1'], targets)

    t.is(targets.rack1.calls[0].error, 'Network error', 'should set error on call')
  })

  t.test('should handle empty targets', async (t) => {
    const net = new MockNetFacility()
    const racks = new MockHyperbee()
    const caller = createActionCaller(net, racks)

    await caller.callTargets('action', ['param1'], {})

    t.is(net.jRequestCalls.length, 0, 'should make no calls')
  })
})
