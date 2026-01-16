import { Request, Response } from 'express';
import { PrismaClient } from '@prisma/client';
import { GraphHopperLocation } from '@betypes/graphhopper';
import { fetchGraphHopperRoute } from '../../../utils/graphhopper';

const prisma = new PrismaClient();

/**
 * @swagger
 * components:
 *   schemas:
 *     PickupLocation:
 *       type: object
 *       properties:
 *         fromLat:
 *           type: number
 *           description: Latitude of the pickup location
 *         fromLong:
 *           type: number
 *           description: Longitude of the pickup location
 * /api/v1/trips/route/{rideId}:
 *   get:
 *     summary: Get pickup locations for a specific ride
 *     description: Returns all the latitude and longitude coordinates for pickups from the specified ride.
 *     tags: ["Trips"]
 *     parameters:
 *       - in: path
 *         name: rideId
 *         required: true
 *         schema:
 *           type: integer
 *         description: The ID of the ride
 *     responses:
 *       200:
 *         description: A list of pickup locations
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/PickupLocation'
 *       404:
 *         description: Ride not found
 *       500:
 *         description: Failed to fetch pickup locations
 */
const getRideRoute = async (req: Request, res: Response) => {
  const { rideId } = req.params;
  if (!rideId) {
    return res.status(400).json({ error: 'Ride ID parameter is required' });
  }

  try {
    const ride = await prisma.ride.findUnique({
      where: { id: parseInt(rideId, 10) },
      include: {
        userRide: {
          include: {
            user: true,
          },
        },
      },
    });

    if (!ride) {
      return res.status(404).json({ error: 'Ride not found' });
    }

    const startLocation: GraphHopperLocation = {
      name: 'real_start_location',
      lat: ride.fromLat,
      long: ride.fromLong
    };

    const endLocation: GraphHopperLocation = {
      name: 'real_end_location',
      lat: ride.toLat,
      long: ride.toLong
    };

    const serviceLocations: GraphHopperLocation[] = ride.userRide.map(userRide => ({
      name: `${userRide.user.firstName}_${userRide.user.lastName}_start`,
      lat: userRide.fromLat,
      long: userRide.fromLong
    })).concat(ride.userRide.map(userRide => ({
      name: `${userRide.user.firstName}_${userRide.user.lastName}_end`,
      lat: userRide.toLat,
      long: userRide.toLong
    })));

    const graphHopperResponse = await fetchGraphHopperRoute(startLocation, endLocation, serviceLocations);

    // Formatting the response to match the expected output, honestly this is a bit of a mess and could be improved
    const formattedLocations = graphHopperResponse.map(location => ({
      userName: location.name.split('_')[0] + ' ' + location.name.split('_')[1], // Assuming the name was formatted as 'firstName_lastName_start' or 'firstName_lastName_end'
      lat: location.lat,
      long: location.long,
      type: location.name.includes('start') ? (location.name.includes('real_start') ? 'start' : 'pickup') : (location.name.includes('real_end') ? 'end' : 'dropoff')
    }));

    res.json(formattedLocations);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Failed to fetch pickup locations' });
  }
};

export { getRideRoute };