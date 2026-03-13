'use strict'

const test = require('brittle')
const fs = require('fs')
const path = require('path')
const worker = require('bfx-svc-boot-js/lib/worker')
const RPC = require('@hyperswarm/rpc')
const { setTimeout: sleep } = require('timers/promises')

const thingServiceRoot = path.resolve(__dirname, '../../../miningos-tpl-wrk-thing/tests/integration')
const orkServiceRoot = path.resolve(__dirname, '../..')

const thingRack = 'rack-1'
const orkCluster = 'cluster-0'

let rpc
let thingRpcClient
let orkRpcClient
let thingWorker
let orkWorker

test('e2e:getThingsCount', { timeout: 120000 }, async (main) => {
  main.teardown(async () => {
    if (rpc) await rpc.destroy()
    if (thingWorker) await thingWorker.stop()
    if (orkWorker) await orkWorker.stop()
    await sleep(1000)

    // cleanup thing worker dirs
    fs.rmSync(path.join(thingServiceRoot, 'store'), { recursive: true, force: true })
    fs.rmSync(path.join(thingServiceRoot, 'status'), { recursive: true, force: true })
    fs.rmSync(path.join(thingServiceRoot, 'config'), { recursive: true, force: true })

    // cleanup ork worker config files (preserve .example files)
    const orkConfigFiles = [
      path.join(orkServiceRoot, 'config/common.json'),
      path.join(orkServiceRoot, 'config/base.ork.json'),
      path.join(orkServiceRoot, 'config/facs/net.config.json')
    ]
    orkConfigFiles.forEach(f => {
      try { fs.unlinkSync(f) } catch (e) {}
    })

    // cleanup store and status dirs (both workers write store relative to cwd)
    fs.rmSync(path.join(orkServiceRoot, 'store'), { recursive: true, force: true })
    fs.rmSync(path.join(orkServiceRoot, 'status'), { recursive: true, force: true })
  })

  // Step 1: Create configs for both workers
  const createThingConfig = () => {
    const configDir = path.join(thingServiceRoot, 'config')
    const facsDir = path.join(configDir, 'facs')
    fs.mkdirSync(facsDir, { recursive: true })

    fs.writeFileSync(
      path.join(configDir, 'common.json'),
      JSON.stringify({ dir_log: 'logs', debug: 0 })
    )
    fs.writeFileSync(
      path.join(configDir, 'base.thing.json'),
      JSON.stringify({
        storeSnapItvMs: 60000,
        collectSnapsItvMs: 60000,
        rotateLogsItvMs: 60000,
        logRotateMaxLength: 4,
        logKeepCount: 1,
        refreshLogsCacheItvMs: 60000
      })
    )
    fs.writeFileSync(
      path.join(facsDir, 'net.config.json'),
      JSON.stringify({ r0: {} })
    )
  }

  const createOrkConfig = () => {
    const configDir = path.join(orkServiceRoot, 'config')
    const facsDir = path.join(configDir, 'facs')
    fs.mkdirSync(facsDir, { recursive: true })

    fs.writeFileSync(
      path.join(configDir, 'common.json'),
      JSON.stringify({ dir_log: 'logs', debug: 0 })
    )
    fs.writeFileSync(
      path.join(configDir, 'base.ork.json'),
      JSON.stringify({
        actionIntvlMs: 30000,
        callTargetsLimit: 50
      })
    )
    fs.writeFileSync(
      path.join(facsDir, 'net.config.json'),
      JSON.stringify({ r0: {} })
    )
  }

  createThingConfig()
  createOrkConfig()

  // Step 2: Boot thing worker
  thingWorker = worker({
    env: 'test',
    wtype: 'wrk-miner-rack-test',
    serviceRoot: thingServiceRoot,
    rack: thingRack
  })
  await sleep(7000)

  // Step 3: Read thing worker's RPC public key
  const thingStatusPath = path.join(
    thingServiceRoot, 'status', `wrk-miner-rack-test-${thingRack}.json`
  )
  const thingStatus = JSON.parse(fs.readFileSync(thingStatusPath, 'utf8'))
  const thingRpcKey = thingStatus.rpcPublicKey

  // Step 4: Boot ork worker
  orkWorker = worker({
    env: 'test',
    wtype: 'wrk-ork-proc-aggr',
    serviceRoot: orkServiceRoot,
    cluster: orkCluster
  })
  await sleep(7000)

  // Step 5: Read ork worker's RPC public key
  const orkStatusPath = path.join(
    orkServiceRoot, 'status', `wrk-ork-proc-aggr-${orkCluster}.json`
  )
  const orkStatus = JSON.parse(fs.readFileSync(orkStatusPath, 'utf8'))
  const orkRpcKey = orkStatus.rpcPublicKey

  // Step 6: Create external RPC client and connect to both workers
  rpc = new RPC()
  thingRpcClient = rpc.connect(Buffer.from(thingRpcKey, 'hex'))
  orkRpcClient = rpc.connect(Buffer.from(orkRpcKey, 'hex'))

  // Step 7: Register 3 things in the thing worker (2 miners, 1 container)
  const things = [
    {
      id: 'miner-1',
      tags: ['t-miner'],
      info: { site: 'test', macAddress: '00:00:00:01' },
      opts: { address: '10.0.0.1', port: 8080, username: 'test', password: 'pass1' }
    },
    {
      id: 'miner-2',
      tags: ['t-miner'],
      info: { site: 'test', macAddress: '00:00:00:02' },
      opts: { address: '10.0.0.2', port: 8080, username: 'test', password: 'pass2' }
    },
    {
      id: 'container-1',
      tags: ['t-container'],
      info: { site: 'test', macAddress: '00:00:00:03' },
      opts: { address: '10.0.0.3', port: 8080, username: 'test', password: 'pass3' }
    }
  ]

  for (const thing of things) {
    await thingRpcClient.request(
      'registerThing',
      Buffer.from(JSON.stringify(thing)),
      { timeout: 5000 }
    )
  }

  // Step 8: Register the thing rack with the ork
  await orkRpcClient.request(
    'registerRack',
    Buffer.from(JSON.stringify({
      id: thingRack,
      type: 'wrk-miner-test',
      info: { rpcPublicKey: thingRpcKey }
    })),
    { timeout: 5000 }
  )

  // Step 9: Assert getThingsCount over real Hyperswarm RPC
  await main.test('getThingsCount: all things', async (t) => {
    const res = await orkRpcClient.request(
      'getThingsCount',
      Buffer.from(JSON.stringify({})),
      { timeout: 10000 }
    )
    const count = JSON.parse(res.toString())
    t.is(count, 3, 'should return total count of 3')
  })

  await main.test('getThingsCount: filter t-miner', async (t) => {
    const res = await orkRpcClient.request(
      'getThingsCount',
      Buffer.from(JSON.stringify({ query: { tags: { $in: ['t-miner'] } } })),
      { timeout: 10000 }
    )
    const count = JSON.parse(res.toString())
    t.is(count, 2, 'should return 2 miners')
  })

  await main.test('getThingsCount: filter t-container', async (t) => {
    const res = await orkRpcClient.request(
      'getThingsCount',
      Buffer.from(JSON.stringify({ query: { tags: { $in: ['t-container'] } } })),
      { timeout: 10000 }
    )
    const count = JSON.parse(res.toString())
    t.is(count, 1, 'should return 1 container')
  })

  await main.test('getThingsCount: filter nonexistent tag', async (t) => {
    const res = await orkRpcClient.request(
      'getThingsCount',
      Buffer.from(JSON.stringify({ query: { tags: { $in: ['t-nonexistent'] } } })),
      { timeout: 10000 }
    )
    const count = JSON.parse(res.toString())
    t.is(count, 0, 'should return 0 for nonexistent tag')
  })
})
