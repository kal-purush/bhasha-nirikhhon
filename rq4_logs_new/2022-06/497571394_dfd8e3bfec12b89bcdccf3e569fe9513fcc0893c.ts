import {useEvolutionChainGet} from "../queries/evolution";

interface EvolutionChainReturnType {
    baseForm: string
    midForm: string
    advanceForm: string
}

const useEvolutionChain = (name: string): EvolutionChainReturnType => {
    const {data} = useEvolutionChainGet(name)

    return {
        baseForm: data?.chain.species.name || '',
        midForm: data?.chain.evolves_to[0].species.name || '',
        advanceForm: data?.chain.evolves_to[0].evolves_to[0]?.species.name || ''
    }
}
export default useEvolutionChain