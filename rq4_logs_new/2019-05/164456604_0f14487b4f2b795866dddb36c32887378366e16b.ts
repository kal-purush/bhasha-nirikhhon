import { v4 } from 'uuid'
import DatabaseError from '@errors/DatabaseError'
import { listHighlightedActivities } from '@handlers/activities/highlights'

jest.mock('@datastore/index', () => ({
  datastore: {
    listHighlightedActivities: jest.fn(() => {}),
  },
}))
const datastoreMock = require('@datastore/index')

test('list highlighted activities with datastore error', async () => {
  datastoreMock.datastore.listHighlightedActivities.mockRejectedValueOnce(new DatabaseError('db error'))
  const response = await listHighlightedActivities({}, {})
  expect(response.statusCode).toBe(500)
})

test('list highlighted activities properly', async () => {
  const data = [{ activityId: v4() }, { activityId: v4() }]
  datastoreMock.datastore.listHighlightedActivities.mockResolvedValueOnce(data)
  const response = await listHighlightedActivities({}, {})
  expect(response.statusCode).toBe(200)
  expect(response.body).toBe(JSON.stringify(data))
})