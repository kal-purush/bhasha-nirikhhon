import R from 'ramda';

const paginate = (data, first, after) => {
  if (first < 0) {
    throw new Error(`Invalid first`);
  }

  const modifiedData = R.map((node: any) => {
    return R.assoc('rawCursor', node.cursor.buffer, node);
  })(data);

  const startingIndex = R.findIndex(
    R.propEq('rawCursor', Buffer.from(after, 'base64')),
  )(modifiedData);

  if (startingIndex < 0) {
    throw new Error(`Invalid cursor`);
  }

  const lastIndex = startingIndex + first;

  const nodes = R.slice(startingIndex, lastIndex, modifiedData);

  const edges = R.map((node: any) => {
    return {
      node,
      cursor: node.cursor,
    };
  })(nodes);

  return {
    totalCount: nodes.length,
    pageInfo: {
      endCursor: edges[nodes.length - 1].cursor.toString('base64'),
      hasNextPage: lastIndex < modifiedData.length,
    },
    edges,
  };
};

export { paginate };