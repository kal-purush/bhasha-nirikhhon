import { v4 } from 'uuid'
import NotFoundError from '@errors/NotFoundError'
import DatabaseError from '@errors/DatabaseError'
import TravelBand from '@models/TravelBand'
import ISpotter from '@models/Spotter'
import { createTravelBand } from '@handlers/travelBands/create'
import { defaultTravelBandThumbnail } from '@constants/defaults'

jest.mock('@datastore/index', () => ({
  datastore: {
    getSpotter: jest.fn((spotterId: string) => {}),
    createTravelBand: jest.fn((travelBand: TravelBand) => {}),
  },
}))
const datastoreMock = require('@datastore/index')

const fixtureSpotter: ISpotter = {
  spotterId: v4(),
  username: 'testing user',
  thumbnailUrl: 'doesnotmatter',
}
const fixtureTravelBand: TravelBand = {
  travelBandId: v4(),
  folders: [],
  bookings: [],
  name: 'testing',
  description: 'testing',
  spotters: [{ ...fixtureSpotter }],
  activityCount: 0,
  thumbnailUrl: defaultTravelBandThumbnail,
}

const checkTravelBand = (travelBand: TravelBand, withDescription = false) => {
  expect(travelBand.activityCount).toBe(0)
  expect(travelBand.bookings.length).toBe(0)
  expect(travelBand.folders.length).toBe(1)
  expect(travelBand.folders[0].isDefault).toBeTruthy()
  expect(travelBand.name).toBe(fixtureTravelBand.name)
  expect(travelBand.thumbnailUrl).toBe(defaultTravelBandThumbnail)
  if (withDescription) {
    expect(travelBand.description).toBe(fixtureTravelBand.description)
  } else {
    expect(travelBand.description).toBeUndefined()
  }
}

const fixtureEvent = {
  body: JSON.stringify({ name: fixtureTravelBand.name, description: fixtureTravelBand.description }),
}

test('create travel band - spotter does not exist', async () => {
  datastoreMock.datastore.getSpotter.mockRejectedValueOnce(new NotFoundError('does not exist'))
  const response = await createTravelBand(fixtureEvent)
  expect(response.statusCode).toBe(404)
})

test('get spotter internal error', async () => {
  datastoreMock.datastore.getSpotter.mockRejectedValueOnce(new DatabaseError('database error'))
  const response = await createTravelBand(fixtureEvent)
  expect(response.statusCode).toBe(500)
})

test('create travel band datastore error', async () => {
  datastoreMock.datastore.getSpotter.mockResolvedValueOnce(fixtureSpotter)
  datastoreMock.datastore.createTravelBand.mockRejectedValueOnce(new DatabaseError('database error'))
  const response = await createTravelBand(fixtureEvent)
  expect(response.statusCode).toBe(500)
})

test('create travel band properly without description', async () => {
  datastoreMock.datastore.getSpotter.mockResolvedValueOnce(fixtureSpotter)
  datastoreMock.datastore.createTravelBand = jest.fn((travelBand: TravelBand) => {
    checkTravelBand(travelBand)
  })
  datastoreMock.datastore.createTravelBand.mockResolvedValueOnce({})
  const body = { name: fixtureTravelBand.name }
  const event = { body: JSON.stringify(body) }
  const response = await createTravelBand(event)
  expect(response.statusCode).toBe(201)
  const responseBody = JSON.parse(response.body)
  checkTravelBand(responseBody)
})

test('create travel band properly with description', async () => {
  datastoreMock.datastore.getSpotter.mockResolvedValueOnce(fixtureSpotter)
  datastoreMock.datastore.createTravelBand = jest.fn((travelBand: TravelBand) => {
    checkTravelBand(travelBand, true)
  })
  datastoreMock.datastore.createTravelBand.mockResolvedValueOnce({})
  const body = { name: fixtureTravelBand.name, description: fixtureTravelBand.description }
  const event = { body: JSON.stringify(body) }
  const response = await createTravelBand(event)
  expect(response.statusCode).toBe(201)
  const responseBody = JSON.parse(response.body)
  checkTravelBand(responseBody, true)
})