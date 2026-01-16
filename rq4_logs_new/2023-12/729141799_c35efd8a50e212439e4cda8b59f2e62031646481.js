import { randomUUID } from "crypto"

export class DatabaseMemory{
#placas_de_video = new Map()

list(search){
    return Array.from(this.#placas_de_video.entries()).map((placas_de_videoArray) =>{
    // acessando primeira posição
        const id = placas_de_videoArray[0]
        const data = placas_de_videoArray[1]

        return{
            id,
            ...data
        }
    })
    .filter(placa_de_video => {
        if (search){
            return placa_de_video.marca.includes(search)
        }
        return true
    })
}
create(placa_de_video){
    const placa_de_videoId = randomUUID()
    this.#placas_de_video.set(placa_de_videoId, placa_de_video)
}
update(id, placa_de_video){
    this.#placas_de_video.set(id, placa_de_video)
}
delete(id, placa_de_video){
    this.#placas_de_video.delete(id, placa_de_video)
}
}