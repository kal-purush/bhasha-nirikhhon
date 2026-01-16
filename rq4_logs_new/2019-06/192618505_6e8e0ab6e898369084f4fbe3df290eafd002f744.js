function CakeError(endereco){ 
    this.endereco = endereco
    this.message = `Não consegui entender o endereço: \n\n${endereco}`
}

export { CakeError }