import { useQuery, gql } from '@apollo/client';
import type { Query } from '@favware/graphql-pokemon';
import { apolloClient } from '../../api/pokemon-apollo-client';

interface GraphQLPokemonResponse<
  K extends keyof Omit<Query, '__typename'>,
> {
  data: Record<K, Omit<Query[K], '__typename'>>;
}

const GET_POKEMON_DETAILS = gql`
  {
    getPokemon(pokemon: vaporeon) {
      sprite
      num
      species
      color
    }
  }
`;

const useGetPokemon = (pokemonKey?: string | number) => {
  const { loading, error, data } = useQuery<
    GraphQLPokemonResponse<'getPokemon'>
  >(GET_POKEMON_DETAILS, {
    client: apolloClient,
  });

  return {
    pokemon: data,
    pokemonLoading: loading,
    pokemonError: error,
  };
};

export default useGetPokemon;