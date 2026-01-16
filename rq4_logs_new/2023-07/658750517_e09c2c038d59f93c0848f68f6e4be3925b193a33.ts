import { useQuery } from "@apollo/client";
import { GET_TRENDING_FEED } from "./queries";
import { Entry } from "../../store/entries/types";

export const useTrendingFeed = (
  tags: string[],
  hive_id: string
): { indexerLoading: boolean; indexerData: Entry[] } => {
  const { loading, data } = useQuery(GET_TRENDING_FEED, {
    variables: {
      tags
    }
  });

  if (!loading) {
    const {
      trendingFeed: { items }
    } = data;

    return {
      indexerLoading: loading,
      indexerData: items.map(
        ({
          author,
          body,
          created_at,
          updated_at,
          stats,
          json_metadata,
          permlink,
          title,
          community
        }: any) =>
          ({
            active_votes: [],
            author: author.username,
            author_payout_value: "0.000 HBD",
            author_reputation: 100,
            beneficiaries: [],
            blacklists: [],
            body,
            category: hive_id,
            children: 0,
            created: created_at,
            community: community["_id"].split("/")[1],
            total_votes: stats ? stats.num_votes : 0,
            json_metadata,
            curator_payout_value: "0.000 HBD",
            depth: 0,
            is_paidout: false,
            max_accepted_payout: "1000000.000 HBD",
            net_rshares: 31735727304640,
            payout: stats ? stats.total_hive_reward : "0.000 HBD",
            payout_at: new Date().toISOString(),
            pending_payout_value: stats ? stats.total_hive_reward : "0.000 HBD",
            percent_hbd: 100,
            permlink,
            post_id: Math.floor(Math.random() * 100),
            promoted: "0.000 HBD",
            replies: [],
            title,
            updated: updated_at,
            url: `/${community._id.split("/")[1]}/@${author.username}/${permlink}`
          } as Entry)
      )
    };
  }

  return {
    indexerData: [],
    indexerLoading: true
  };
};