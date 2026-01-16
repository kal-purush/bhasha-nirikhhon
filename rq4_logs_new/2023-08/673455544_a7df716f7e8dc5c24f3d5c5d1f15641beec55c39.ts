export const numberToMoney = (value: number) => {

    let result: string = value.toLocaleString()

    result = `R$ ${result},00`

    return result
}

export const handleValue = (event: React.FormEvent<HTMLInputElement>) => {
    const character = event.currentTarget

    character.value = valueMask(character.value)
}

function valueMask(value: string) {
    if (!value) return ''

    value = value.replace(/\D/g, '')
    value = value.replace(/(\d{0})(\d)/, "$1 R$ $2")
    value = value.replace(/(\d)(\d{8})$/, "$1.$2")
    value = value.replace(/(\d)(\d{5})$/, "$1.$2")
    value = value.replace(/(\d)(\d{2})$/, "$1,$2")

    return value
}