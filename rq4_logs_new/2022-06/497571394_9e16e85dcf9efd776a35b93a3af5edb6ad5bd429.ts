import {dehydrate, QueryClient} from 'react-query';
import {QUERY_KEYS} from "../utils/queryKeys";
import {fetchAllPokemons, fetchPokemons} from "../queries/pokemon";

const prefetchAllPokemons = (client: QueryClient) => client.prefetchQuery(QUERY_KEYS.POKEMON_POKEMONS_FULL_LIST, fetchAllPokemons)
const prefetchFirst20PokemonNames = (client: QueryClient) => client.prefetchInfiniteQuery(QUERY_KEYS.POKEMON_POKEMONS, fetchPokemons)

export const getServerSideProps = async () => {
    const queryClient = new QueryClient()

    const prefetchedCallbacks = [prefetchAllPokemons, prefetchFirst20PokemonNames]
    await Promise.allSettled(prefetchedCallbacks.map((prefetch) => prefetch(queryClient)))

    // That fixes the serialize problem from nextjs
    queryClient.setQueryData(QUERY_KEYS.POKEMON_POKEMONS, (data) => ({
        // @ts-ignore
        ...data,
        pageParams: [],
    }));


    return {
        props: {
            dehydratedState: dehydrate(queryClient),
        },
    }
}