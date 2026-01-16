export const queries = {
  LAST_30_DAYS_UNIQUE_BY_ADDRESS: [
    {
      $match: {
        lastSeen: {
          $gte: new Date(+new Date() - 30 * 24 * 60 * 60 * 1000),
          $lte: new Date()
        }
      }
    },
    {
      $sort: {
        lastSeen: -1
      }
    },
    {
      $group: {
        _id: "$address",
        lastSeen: { $first: "$lastSeen" },
        doc: { $first: "$$ROOT" }
      }
    },
    {
      $replaceRoot: {
        newRoot: "$doc"
      }
    },
    {
      $limit: 1000
    }
  ],
};

export const aggregations = {
  uniqueBy: (term: string = 'address', count = 1000) => {
    return [
      {
        $group: {
          _id: `$${term}`,
          latest_record: {
            $first: {
              price: "$price"
            }
          }
        }
      },
      {
        $project: {
          _id: 0,
          term: "$_id",
          latest_record: 1
        }
      },
      {
        $limit: count
      }
    ];
  },
};