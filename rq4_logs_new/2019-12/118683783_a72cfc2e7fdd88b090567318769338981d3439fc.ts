import { Policy } from 'common/typedefs/Policy';
import { isNil, omitBy } from 'lodash';
import queryString from 'querystring';
import ajax from 'services/ajax';

export const getPolicies = ({
  offset = 0,
  limit = 20,
  query = null,
  sortField = null,
  sortOrder = null,
  status = null,
}): Promise<{ count: number; resultSet: Policy[] }> => {
  return ajax
    .get(
      `/policies?${queryString.stringify(
        omitBy(
          {
            limit,
            offset,
            query,
            sort: sortField,
            sortOrder,
          },
          isNil,
        ),
      )}`,
    )
    .then(r => r.data);
};