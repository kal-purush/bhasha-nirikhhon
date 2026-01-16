import { PaginationInputError } from '@custom-errors';
import rType from 'ramda';
import { Connection } from '@types';

export type PaginateInput = {
  model: any;
  first: number;
  after: string;
};

const makePaginateDbLayer = ({ R }: { R: typeof rType }) => {
  return async function <T>({
    model,
    first,
    after,
  }: PaginateInput): Promise<Connection<T>> {
    let limit: number;

    if (first < 0) {
      throw new PaginationInputError(`Invalid first`);
    }

    if (first) {
      limit = first;
    } else {
      limit = Number.MAX_SAFE_INTEGER;
    }

    let startingDate = new Date(-1);

    if (after) {
      startingDate = new Date(Buffer.from(after, 'base64').toString('ascii'));

      if (Number.isNaN(startingDate.getTime())) {
        throw new PaginationInputError(`Invalid cursor`);
      }
    }

    const [{ data, total }] = await model.aggregate([
      {
        $facet: {
          data: [
            { $match: { createdAt: { $gte: startingDate } } },
            { $sort: { createdAt: 1 } },
            { $limit: limit },
          ],
          total: [{ $count: 'totalCount' }],
        },
      },
    ]);

    const totalDocumentCount = total[0].totalCount;

    const edges = R.map((node: any) => {
      return {
        node: R.assoc('id', node._id, node),
        cursor: node.cursor,
      };
    })(data);

    return {
      totalCount: data.length,
      pageInfo: {
        endCursor: edges[edges.length - 1].cursor,
        hasNextPage: edges.length < totalDocumentCount,
      },
      edges,
    };
  };
};

export default makePaginateDbLayer;