import { Ride } from '@communetypes/Ride';
import { graphqlRequest } from '../graphql';
import { EditRideSchema } from '@communetypes/EditRideSchema';
import { updateRideQuery } from '../utils/userRideQueries';

interface GraphQLRideResponse {
  updateRideById: {
    ride: Ride;
  };
}

const postUpdateRide = async (ride: EditRideSchema): Promise<Ride> => {
  const query = updateRideQuery(ride);

  try {
    const responseData = await graphqlRequest<GraphQLRideResponse>(query);
    return responseData.updateRideById.ride;
  } catch (error) {
    console.error('Error updating ride:', error);
    throw error;
  }
};

export { postUpdateRide };