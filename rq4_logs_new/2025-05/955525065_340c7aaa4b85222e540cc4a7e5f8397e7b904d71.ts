function getDateAsNumber(date: string | undefined) {
    let D: number = 0, M: number = 0, Y: number = 0
    if(!date) return { D, M, Y }
    const arr = date.split("-")
    Y = parseInt(arr[0]), M = parseInt(arr[1]), D = parseInt(arr[2])
    return { D, M, Y }
}
export default getDateAsNumber