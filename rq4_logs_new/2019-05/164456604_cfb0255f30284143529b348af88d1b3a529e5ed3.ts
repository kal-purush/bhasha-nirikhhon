import { v4 } from 'uuid'
import NotFoundError from '@errors/NotFoundError'
import DatabaseError from '@errors/DatabaseError'
import ISpotter from '@models/Spotter'
import { inviteSpotter } from '@handlers/spotters/invite'
import { APIGatewayProxyEvent } from 'aws-lambda'
import UnauthorizedError from '@errors/UnauthorizedError'
import BadRequestError from '@errors/BadRequestError'

// authorizations
jest.mock('@handlers/utils/auth', () => ({
  isAuthorizedFromEvent: jest.fn((event: APIGatewayProxyEvent) => {}),
}))
const authMock = require('@handlers/utils/auth')

jest.mock('@datastore/index', () => ({
  datastore: {
    inviteSpotterToTravelBand: jest.fn((spotterId: string, travelBandId: string) => {}),
  },
}))
const datastoreMock = require('@datastore/index')

const fixtureLoggedInSpotterId = v4()
const fixtureTravelBandId = v4()
const fixtureSpotter: ISpotter = {
  spotterId: v4(),
  username: 'testing user',
  email: 'testing@testing.fr',
  thumbnailUrl: 'doesnotmatter',
  travelBands: [fixtureTravelBandId],
}
const fixtureEvent: APIGatewayProxyEvent = {
  queryStringParameters: {},
  isBase64Encoded: false,
  httpMethod: 'POST',
  // @ts-ignore
  requestContext: {},
  stageVariables: {},
  body: JSON.stringify({ spotterId: fixtureSpotter.spotterId }),
  headers: {
    'X-Spotter': fixtureLoggedInSpotterId,
  },
  pathParameters: {
    travelBandId: fixtureTravelBandId,
  },
}

test('not authorized', async () => {
  authMock.isAuthorizedFromEvent.mockRejectedValueOnce(new UnauthorizedError('not authorized'))
  const response = await inviteSpotter(fixtureEvent)
  expect(response.statusCode).toBe(401)
})

test('spotter does not exist', async () => {
  console.log(datastoreMock)
  authMock.isAuthorizedFromEvent.mockResolvedValueOnce(null)
  datastoreMock.datastore.inviteSpotterToTravelBand.mockRejectedValueOnce(new NotFoundError('does not exist'))
  const response = await inviteSpotter(fixtureEvent)
  expect(response.statusCode).toBe(404)
})

test('invite spotter internal error', async () => {
  datastoreMock.datastore.inviteSpotterToTravelBand.mockRejectedValueOnce(new DatabaseError('database error'))
  const response = await inviteSpotter(fixtureEvent)
  expect(response.statusCode).toBe(500)
})

test('spotter id to invite not provided in event', async () => {
  const event = {
    ...fixtureEvent,
    body: '{}',
  }
  const response = await inviteSpotter(event)
  expect(response.statusCode).toBe(400)
})

test('invalid travel band id', async () => {
  const event = {
    ...fixtureEvent,
    pathParameters: {
      travelBandId: '',
    },
  }
  const response = await inviteSpotter(event)
  expect(response.statusCode).toBe(400)
})

test('spotter already invited to travel band', async () => {
  authMock.isAuthorizedFromEvent.mockResolvedValueOnce(null)
  datastoreMock.datastore.inviteSpotterToTravelBand.mockRejectedValueOnce(new BadRequestError('Already in travel band'))
  const response = await inviteSpotter(fixtureEvent)
  expect(response.statusCode).toBe(400)
})

test('invite spotter properly', async () => {
  authMock.isAuthorizedFromEvent.mockResolvedValueOnce(null)
  datastoreMock.datastore.inviteSpotterToTravelBand.mockResolvedValueOnce(fixtureSpotter)
  const response = await inviteSpotter(fixtureEvent)
  expect(response.statusCode).toBe(200)
  expect(response.body).toBe(JSON.stringify(fixtureSpotter))
})