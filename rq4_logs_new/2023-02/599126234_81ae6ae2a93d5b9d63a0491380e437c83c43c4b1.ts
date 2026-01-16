import { useQuery, gql } from "@apollo/client";
import type { Query } from "@favware/graphql-pokemon";
import { apolloClient } from "../../api/pokemon-apollo-client";

interface GraphQLPokemonResponse<K extends keyof Omit<Query, "__typename">> {
  data: Record<K, Omit<Query[K], "__typename">>;
}

const useGetPokemon = (pokemonKey?: string | number) => {
  const GET_POKEMON = gql`
    {
        getPokemon(pokemon: ${pokemonKey}) {
          key
          num
          sprite
          shinySprite
          weight
          height
          species
          baseStatsTotal
          sprite
          shinySprite
          evolutionLevel
          types {
            name
          }
          abilities {
            first {
              name
            }
            hidden {
              name
            }
            second {
              name
            }
            special {
              name
            }
          }
          baseStats {
            attack
            defense
            hp
            specialattack
            specialdefense
            speed
          }
          moves {
            key
            desc
          }
        }
    }
    `;

  const { loading, error, data } = useQuery<GraphQLPokemonResponse<"getPokemon">>(GET_POKEMON, {
    client: apolloClient,
  });
  // console.log('useGetPokemon data: ', data);

  return {
    pokemon: data,
    pokemonLoading: loading,
    pokemonError: error,
  };
};

export default useGetPokemon;