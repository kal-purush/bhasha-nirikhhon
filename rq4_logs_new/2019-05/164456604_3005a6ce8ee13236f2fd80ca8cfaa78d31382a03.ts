import { datastore } from '@datastore/index';
import { handleError } from '@helpers/http'

/**
 * Retrieve a list of highlighted activities
 * @param event - Event containing Query Parameters for pagination:
 * - lastEvaluatedId: ID of the last retrieved activity if not on the first page
 * - itemsPerPage: Number of items to retrieve per page
 * @param context
 * @returns a list of highlighted activities
 */
export const listHighlightedActivities = async (event, context) => {
  try {
    const activities = await datastore.listHighlightedActivities()

    return {
      statusCode: 200,
      body: JSON.stringify(activities),
    }
  } catch (e) {
    return handleError(e)
  }
}