import { v4 } from 'uuid'
import NotFoundError from '@errors/NotFoundError'
import DatabaseError from '@errors/DatabaseError'
import ISpotter from '@models/Spotter'
import { createReaction } from '@handlers/travelBands/reactions/createReaction'
import Activity from '@models/Activity'

jest.mock('@datastore/index', () => ({
  datastore: {
    getSpotter: jest.fn((spotterId: string) => {}),
    getTravelBandActivity: jest.fn((travelBandId: string, activityId: string) => {}),
    updateTravelBandActivity: jest.fn((activity: Activity) => {
      expect(activity.reactions.length).toBe(1)
    }),
  },
}))
const datastoreMock = require('@datastore/index')

const fixtureTravelBandId = v4()
const fixtureSpotter: ISpotter = {
  spotterId: v4(),
  username: 'testing user',
  thumbnailUrl: 'doesnotmatter',
  travelBands: [fixtureTravelBandId],
}
const fixtureActivity = {
  travelBandId: fixtureTravelBandId,
  activityId: v4(),
  reactions: [],
}
const fixtureEvent = {
  body: JSON.stringify({ like: true }),
  pathParameters: {
    activityId: fixtureActivity.activityId,
    travelBandId: fixtureTravelBandId,
  },
}

test('spotter does not exist', async () => {
  datastoreMock.datastore.getSpotter.mockRejectedValueOnce(new NotFoundError('does not exist'))
  const response = await createReaction(fixtureEvent)
  expect(response.statusCode).toBe(404)
})

test('get spotter internal error', async () => {
  datastoreMock.datastore.getSpotter.mockRejectedValueOnce(new DatabaseError('database error'))
  const response = await createReaction(fixtureEvent)
  expect(response.statusCode).toBe(500)
})

test('like not provided in event', async () => {
  const response = await createReaction({})
  expect(response.statusCode).toBe(400)

  const noLikeResponse = await createReaction({ ...fixtureEvent, body: JSON.stringify({ test: true }) })
  expect(noLikeResponse.statusCode).toBe(400)
})

test('invalid travel band id', async () => {
  const event = {
    ...fixtureEvent,
    pathParameters: {
      activityId: fixtureActivity.activityId,
      travelBandId: '',
    },
  }
  const response = await createReaction(event)
  expect(response.statusCode).toBe(400)
})

test('invalid activity id', async () => {
  const event = {
    ...fixtureEvent,
    pathParameters: {
      activityId: '',
      travelBandId: fixtureTravelBandId,
    },
  }
  const response = await createReaction(event)
  expect(response.statusCode).toBe(400)
})

test('spotter not invited to travel band', async () => {
  datastoreMock.datastore.getSpotter.mockResolvedValueOnce({ ...fixtureSpotter, travelBands: [] })
  const response = await createReaction(fixtureEvent)
  expect(response.statusCode).toBe(401)
})

test('get travel band activity not found', async () => {
  datastoreMock.datastore.getSpotter.mockResolvedValueOnce(fixtureSpotter)
  datastoreMock.datastore.getTravelBandActivity.mockRejectedValueOnce(new NotFoundError('not found'))
  const response = await createReaction(fixtureEvent)
  expect(response.statusCode).toBe(404)
})

test('get travel band activity db error', async () => {
  datastoreMock.datastore.getSpotter.mockResolvedValueOnce(fixtureSpotter)
  datastoreMock.datastore.getTravelBandActivity.mockRejectedValueOnce(new DatabaseError('db error'))
  const response = await createReaction(fixtureEvent)
  expect(response.statusCode).toBe(500)
})

test('add reaction to activity properly - liked then unliked', async () => {
  datastoreMock.datastore.getSpotter.mockResolvedValueOnce(fixtureSpotter)
  datastoreMock.datastore.getTravelBandActivity.mockResolvedValueOnce(fixtureActivity)
  datastoreMock.datastore.updateTravelBandActivity.mockResolvedValueOnce(null)
  const response = await createReaction(fixtureEvent)
  expect(response.statusCode).toBe(201)
  const data = JSON.parse(response.body)
  expect(data.username).toBe(fixtureSpotter.username)
  expect(data.spotterId).toBe(fixtureSpotter.spotterId)
  expect(data.like).toBe(true)

  // unlike
  datastoreMock.datastore.getSpotter.mockResolvedValueOnce(fixtureSpotter)
  datastoreMock.datastore.getTravelBandActivity.mockResolvedValueOnce(fixtureActivity)
  datastoreMock.datastore.updateTravelBandActivity.mockResolvedValueOnce(null)
  const unlikeResponse = await createReaction({ ...fixtureEvent, body: JSON.stringify({ like: false }) })
  expect(response.statusCode).toBe(201)
  const unlikeData = JSON.parse(unlikeResponse.body)
  expect(unlikeData.username).toBe(fixtureSpotter.username)
  expect(unlikeData.spotterId).toBe(fixtureSpotter.spotterId)
  expect(unlikeData.like).toBe(false)
})

test('add reaction to activity database error', async () => {
  datastoreMock.datastore.getSpotter.mockResolvedValueOnce(fixtureSpotter)
  datastoreMock.datastore.getTravelBandActivity.mockResolvedValueOnce(fixtureActivity)
  datastoreMock.datastore.updateTravelBandActivity.mockRejectedValueOnce(new DatabaseError('db erro'))
  const response = await createReaction(fixtureEvent)
  expect(response.statusCode).toBe(500)
})