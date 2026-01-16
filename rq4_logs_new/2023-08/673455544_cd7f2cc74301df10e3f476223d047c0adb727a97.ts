export const handlePhone = (event: React.FormEvent<HTMLInputElement>) => {
    const character = event.currentTarget

    character.value = phoneMask(character.value)
}

function phoneMask(value: string) {
    if (!value) return ''

    value = value.replace(/\D/g, '')
    value = value.replace(/(\d{2})(\d)/, "($1) $2")
    value = value.replace(/(\d)(\d{8})$/, "$1 $2")
    value = value.replace(/(\d)(\d{4})$/, "$1-$2")

    return value
}

export const handleCpf = (event: React.FormEvent<HTMLInputElement>) => {
    const character = event.currentTarget

    character.value = cpfMask(character.value)
}

function cpfMask(value: string) {
    if (!value) return ''

    value = value.replace(/\D/g, '')
    value = value.replace(/(\d{3})(\d)/, "$1.$2")
    value = value.replace(/(\d{3})(\d)/, "$1.$2")
    value = value.replace(/(\d)(\d{2})$/, "$1-$2")

    return value
}

export const handleBirthDate = (event: React.FormEvent<HTMLInputElement>) => {
    const character = event.currentTarget

    character.value = birthDateMask(character.value)
}

function birthDateMask(value: string) {
    if (!value) return ''

    value = value.replace(/\D/g, '')
    value = value.replace(/(\d{2})(\d)/, "$1/$2")
    value = value.replace(/(\d{2})(\d)/, "$1/$2")

    return value
}

export const handleCep = (event: React.FormEvent<HTMLInputElement>) => {
    const character = event.currentTarget

    character.value = cepMask(character.value)
}

function cepMask(value: string) {
    if (!value) return ''

    value = value.replace(/\D/g, '')
    value = value.replace(/(\d{5})(\d)/, "$1-$2")

    return value
}

export const handleState = (event: React.FormEvent<HTMLInputElement>) => {
    const character = event.currentTarget

    character.value = stateMask(character.value)
}

function stateMask(value: string) {
    if (!value) return ''

    value = value.replace(/[^a-zA-Z]/, '')
    value = value.replace(value, value.toUpperCase())

    return value
}

export const handleNumber = (event: React.FormEvent<HTMLInputElement>) => {
    const character = event.currentTarget

    character.value = numberMask(character.value)
}

function numberMask(value: string) {
    if (!value) return ''

    value = value.replace(/\D/g, '')

    return value
}