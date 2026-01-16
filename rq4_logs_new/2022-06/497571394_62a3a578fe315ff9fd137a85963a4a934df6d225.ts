import {usePokemonNames} from "./usePokemonNames";
import useAllPokemonNames from "./useAllPokemonNames";
import useSearch from "./useSearch";

const useFilterPokemonNames = () => {
    const {names: defaultNames} = usePokemonNames()
    const {names} = useAllPokemonNames()

    const {searchString, setSearchString} = useSearch()

    const pokemonNames = Boolean(searchString) ? names.filter((name) => name.toLowerCase().includes(searchString)) : defaultNames

    return {pokemonNames, setPokemonName: setSearchString}
}

export default useFilterPokemonNames