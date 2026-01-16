

export const sumar = (numA: number, numB: number) => {
    if (numA < 0) {
        throw new Error("No puede ingresar negativos")
    }
    if (Number.isNaN(numA) || Number.isNaN(numB)) {
        return undefined
    }
    return numA + numB
}

export const restar = (numA: number, numB: number) => {
    if (numA < 0) {
        throw new Error("No puede ingresar negativos")
    }
    if (Number.isNaN(numA) || Number.isNaN(numB)) {
        return undefined
    }
    return numA - numB
}


export class UserController {

    constructor(){

    }

}
