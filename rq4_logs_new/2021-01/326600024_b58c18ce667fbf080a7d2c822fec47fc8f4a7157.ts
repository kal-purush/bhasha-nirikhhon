import rType from 'ramda';
import { Connection } from '../types';

const makePaginate = ({ R }: { R: typeof rType }) => {
  return function <T extends { cursorBuffer: Buffer }>({
    data,
    first,
    after,
  }: {
    data: T[];
    first: number;
    after: string;
  }): Connection<T> {
    if (first < 0) {
      throw new Error(`Invalid first`);
    }

    const startingIndex = R.findIndex(
      R.propEq('cursorBuffer', Buffer.from(after, 'base64')),
    )(data);

    if (startingIndex < 0) {
      throw new Error(`Invalid cursor`);
    }

    const lastIndex = startingIndex + first;

    const nodes = R.slice(startingIndex, lastIndex, data);

    const edges = R.map((node: any) => {
      return {
        node,
        cursor: node.cursor,
      };
    })(nodes);

    return {
      totalCount: nodes.length,
      pageInfo: {
        endCursor: edges[nodes.length - 1].cursor,
        hasNextPage: lastIndex < data.length,
      },
      edges,
    };
  };
};

export default makePaginate;