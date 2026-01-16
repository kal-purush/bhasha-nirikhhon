export const initFields = (fieldSize, initialPosition) => {
    const fields = []
    for (let i = 0; i < fieldSize; i++) {
        const cols = new Array(fieldSize).fill('')
        fields.push(cols)
    }
    fields[17][17] = 'snake'
    fields[initialPosition.y][initialPosition.x] =  'snake'

    return fields
}