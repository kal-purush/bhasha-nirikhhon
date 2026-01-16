import { useQuery, gql } from '@apollo/client';
import type { Query } from '@favware/graphql-pokemon';
import { apolloClient } from '../../api/pokemon-apollo-client';

interface GraphQLPokemonResponse<
  K extends keyof Omit<Query, '__typename'>,
> {
  data: Record<K, Omit<Query[K], '__typename'>>;
}

const useGetPokemon = (pokemonKey?: string | number) => {
  const GET_POKEMON = gql`
    {
        getAllPokemon(pokemon: ${pokemonKey}) {
          sprite
          num
          species
          color
        }
    }
    `;

  const { loading, error, data } = useQuery<
    GraphQLPokemonResponse<'getAllPokemon'>
  >(GET_POKEMON, {
    client: apolloClient,
  });

  return {
    pokemon: data,
    pokemonLoading: loading,
    pokemonError: error,
  };
};

export default useGetPokemon;