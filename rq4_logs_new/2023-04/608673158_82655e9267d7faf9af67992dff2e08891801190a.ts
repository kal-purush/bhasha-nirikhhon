/* eslint-disable @typescript-eslint/consistent-type-assertions */
import { Client, DirectionsResponse, GeocodeResponse, Status, TravelMode } from '@googlemaps/google-maps-services-js';
import { Test, TestingModule } from '@nestjs/testing';
import { GoogleService } from '../google.service';

describe('GoogleService', () => {
  let googleService: GoogleService;
  let clientMock: jest.Mocked<Client>;

  beforeAll(async () => {
    clientMock = {
      geocode: jest.fn(),
      directions: jest.fn(),
    } as any;

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        GoogleService,
        {
          provide: Client,
          useValue: clientMock,
        },
      ],
    }).compile();

    googleService = module.get<GoogleService>(GoogleService);
  });

  afterEach(() => {
    jest.resetAllMocks();
  });

  describe('formatLocation', () => {
    it('should format location', async () => {
      clientMock.geocode.mockResolvedValue({
        data: {
          status: Status.OK,
          results: [{ formatted_address: 'Paris, France' }],
        },
      } as GeocodeResponse);

      const result = await googleService.formatLocation('Paris');

      expect(clientMock.geocode).toHaveBeenCalledWith({
        params: {
          address: 'Paris',
          key: expect.any(String),
        },
      });
      expect(result).toEqual('Paris, France');
    });
    it('should do nothing location (invalid location)', async () => {
      clientMock.geocode.mockResolvedValue({
        data: {
          status: Status.ZERO_RESULTS,
          results: [],
        } as unknown,
      } as GeocodeResponse);

      const result = await googleService.formatLocation('XXXXXXXXXX');

      expect(clientMock.geocode).toHaveBeenCalledWith({
        params: {
          address: 'XXXXXXXXXX',
          key: expect.any(String),
        },
      });
      expect(result).toEqual('XXXXXXXXXX');
    });
  });

  describe('calculateTravel', () => {
    it('should calculate travel duration and departure time', async () => {
      const from = 'Paris, France';
      const to = 'Berlin, Germany';
      const option = {
        mode: TravelMode.driving,
        arrivalTime: new Date('2023-05-01T12:00:00Z'),
      };
      clientMock.directions.mockResolvedValueOnce({
        data: {
          status: Status.OK,
          routes: [
            {
              legs: [{ duration: { value: 1000 } }],
            },
          ],
        },
      } as DirectionsResponse);
      clientMock.directions.mockResolvedValueOnce({
        data: {
          status: Status.OK,
          routes: [
            {
              legs: [{ duration: { value: 800 } }],
            },
          ],
        },
      } as DirectionsResponse);

      const result = await googleService.calculateTravel(from, to, option);

      expect(clientMock.directions).toHaveBeenCalledTimes(2);
      expect(clientMock.directions).toHaveBeenCalledWith({
        params: expect.objectContaining({
          origin: from,
          destination: to,
          mode: option.mode,
          departure_time: option.arrivalTime.getTime(),
        }),
      });
      expect(clientMock.directions).toHaveBeenCalledWith({
        params: expect.objectContaining({
          origin: from,
          destination: to,
          mode: option.mode,
          departure_time: expect.any(Number),
        }),
      });
      expect(result).toEqual({ duration: 800, departureTime: new Date(option.arrivalTime.getTime() - 1000 * 1000) });
    });
  });
});