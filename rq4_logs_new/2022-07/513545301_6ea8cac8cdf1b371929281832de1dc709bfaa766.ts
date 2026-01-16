import { Jogador } from "./jogador"
import { Jogo } from "./jogo"

export interface Apontamento {
    idApontamento: number
    tentativas: number
    data: Date
    jogador?: Jogador
    jogo?: Jogo
}