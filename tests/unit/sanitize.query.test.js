'use strict'

const test = require('brittle')
const WrkProcAggr = require('../../workers/aggr.proc.ork.wrk')

const sanitize = (obj) => WrkProcAggr.prototype._sanitizeMingoQuery(obj)

test('_sanitizeMingoQuery - allows safe comparison operators', (t) => {
  const query = { status: { $eq: 'pending' }, count: { $gt: 5, $lte: 100 } }
  sanitize(query)
  t.pass('should not throw for safe operators')
})

test('_sanitizeMingoQuery - allows logical operators', (t) => {
  const query = { $and: [{ status: { $eq: 'done' } }, { count: { $lt: 10 } }] }
  sanitize(query)
  t.pass('should not throw for $and')

  const query2 = { $or: [{ a: { $in: [1, 2] } }, { b: { $nin: [3] } }] }
  sanitize(query2)
  t.pass('should not throw for $or/$in/$nin')
})

test('_sanitizeMingoQuery - allows $exists and $type', (t) => {
  const query = { name: { $exists: true }, age: { $type: 'number' } }
  sanitize(query)
  t.pass('should not throw for $exists/$type')
})

test('_sanitizeMingoQuery - allows short $regex', (t) => {
  const query = { name: { $regex: 'test.*pattern', $options: 'i' } }
  sanitize(query)
  t.pass('should not throw for short regex')
})

test('_sanitizeMingoQuery - allows $elemMatch and $not', (t) => {
  const query = { items: { $elemMatch: { qty: { $gt: 5 } } } }
  sanitize(query)
  t.pass('should not throw for $elemMatch')

  const query2 = { status: { $not: { $eq: 'error' } } }
  sanitize(query2)
  t.pass('should not throw for $not')
})

test('_sanitizeMingoQuery - blocks $where', (t) => {
  try {
    sanitize({ $where: 'this.a > 1' })
    t.fail('should throw')
  } catch (err) {
    t.is(err.message, 'ERR_QUERY_OPERATOR_NOT_ALLOWED', 'should block $where')
  }
  t.pass()
})

test('_sanitizeMingoQuery - blocks $expr', (t) => {
  try {
    sanitize({ $expr: { $gt: ['$a', '$b'] } })
    t.fail('should throw')
  } catch (err) {
    t.is(err.message, 'ERR_QUERY_OPERATOR_NOT_ALLOWED', 'should block $expr')
  }
  t.pass()
})

test('_sanitizeMingoQuery - blocks unknown $ operators', (t) => {
  try {
    sanitize({ status: { $fakeOp: 1 } })
    t.fail('should throw')
  } catch (err) {
    t.is(err.message, 'ERR_QUERY_OPERATOR_NOT_ALLOWED', 'should block unknown ops')
  }
  t.pass()
})

test('_sanitizeMingoQuery - blocks long $regex', (t) => {
  try {
    sanitize({ name: { $regex: 'a'.repeat(201) } })
    t.fail('should throw')
  } catch (err) {
    t.is(err.message, 'ERR_QUERY_REGEX_TOO_LONG', 'should block long regex')
  }
  t.pass()
})

test('_sanitizeMingoQuery - allows $regex at max length', (t) => {
  sanitize({ name: { $regex: 'a'.repeat(200) } })
  t.pass('should not throw for regex at exactly max length')
})

test('_sanitizeMingoQuery - catches nested $where in $or', (t) => {
  try {
    sanitize({ $or: [{ a: 1 }, { b: { $where: 'bad' } }] })
    t.fail('should throw')
  } catch (err) {
    t.is(err.message, 'ERR_QUERY_OPERATOR_NOT_ALLOWED', 'should catch nested $where')
  }
  t.pass()
})

test('_sanitizeMingoQuery - handles null, undefined, and primitives', (t) => {
  sanitize(null)
  sanitize(undefined)
  sanitize('string')
  sanitize(42)
  sanitize(true)
  t.pass('should handle non-object inputs gracefully')
})

test('_sanitizeMingoQuery - handles empty object', (t) => {
  sanitize({})
  t.pass('should handle empty object')
})

test('_sanitizeMingoQuery - allows plain field values', (t) => {
  const query = { status: 'active', count: 5, active: true }
  sanitize(query)
  t.pass('should not throw for plain field values')
})

// _filterData tests
const filterData = (data, req) => WrkProcAggr.prototype._filterData(data, req)

test('_filterData - returns data unchanged when no query or fields', (t) => {
  const data = [{ id: 1, name: 'a' }, { id: 2, name: 'b' }]
  const result = filterData(data)
  t.is(result.length, 2, 'should return all data')
  t.pass()
})

test('_filterData - filters data with query', (t) => {
  const data = [
    { id: 1, status: 'active' },
    { id: 2, status: 'done' },
    { id: 3, status: 'active' }
  ]
  const result = filterData(data, { query: { status: { $eq: 'active' } } })
  t.is(result.length, 2, 'should return matching items')
  t.is(result[0].id, 1, 'should include first match')
  t.is(result[1].id, 3, 'should include second match')
  t.pass()
})

test('_filterData - projects fields', (t) => {
  const data = [{ id: 1, name: 'a', secret: 'x' }]
  const result = filterData(data, { fields: { id: 1, name: 1 } })
  t.is(result.length, 1, 'should return one item')
  t.ok(result[0].id, 'should include id')
  t.ok(result[0].name, 'should include name')
  t.pass()
})

test('_filterData - handles empty data array', (t) => {
  const result = filterData([], { query: { status: 'active' } })
  t.is(result.length, 0, 'should return empty array')
  t.pass()
})

// queryActions validation tests
test('queryActions - rejects non-array queries', async (t) => {
  const instance = Object.create(WrkProcAggr.prototype)
  try {
    await instance.queryActions({ queries: 'not-an-array' })
    t.fail('should throw')
  } catch (err) {
    t.is(err.message, 'ERR_QUERIES_INVALID', 'should throw ERR_QUERIES_INVALID')
  }
  t.pass()
})

test('queryActions - rejects too many queries', async (t) => {
  const instance = Object.create(WrkProcAggr.prototype)
  const queries = Array.from({ length: 51 }, (_, i) => ({ type: `type-${i}` }))
  try {
    await instance.queryActions({ queries })
    t.fail('should throw')
  } catch (err) {
    t.is(err.message, 'ERR_QUERIES_LIMIT_EXCEEDED', 'should throw ERR_QUERIES_LIMIT_EXCEEDED')
  }
  t.pass()
})

test('queryActions - rejects query with missing type', async (t) => {
  const instance = Object.create(WrkProcAggr.prototype)
  try {
    await instance.queryActions({ queries: [{ filter: {} }] })
    t.fail('should throw')
  } catch (err) {
    t.is(err.message, 'ERR_QUERIES_TYPE_INVALID', 'should throw ERR_QUERIES_TYPE_INVALID')
  }
  t.pass()
})

test('queryActions - rejects query with non-string type', async (t) => {
  const instance = Object.create(WrkProcAggr.prototype)
  try {
    await instance.queryActions({ queries: [{ type: 123 }] })
    t.fail('should throw')
  } catch (err) {
    t.is(err.message, 'ERR_QUERIES_TYPE_INVALID', 'should throw ERR_QUERIES_TYPE_INVALID')
  }
  t.pass()
})
