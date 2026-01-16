import { useQuery } from "../useQuery";
import { GET_MY_OFFERS } from "$queries";
import { IOffer } from "$interfaces/Offer";
import { FetchResult } from "$interfaces/Pagination";
import { IPaginatedResult } from "./interface";

export const useMyOffers = () => {
  const result = useQuery<{}, IUseMyOffers>(GET_MY_OFFERS);

  const fetchMore = () => {
    const offers = result.data?.getMyOffers.results;
    if (!offers) return;
    const lastOffer = offers[offers.length - 1];
    return result.fetchMore({
      query: GET_MY_OFFERS,
      variables: {
        updatedBeforeThan: {
          dateTime: lastOffer.updatedAt,
          uuid: lastOffer.uuid
        }
      }
    }) as FetchResult<IUseMyOffers>;
  };

  return { ...result, fetchMore };
};

interface IUseMyOffers {
  getMyOffers: IPaginatedResult<IOffer>;
}