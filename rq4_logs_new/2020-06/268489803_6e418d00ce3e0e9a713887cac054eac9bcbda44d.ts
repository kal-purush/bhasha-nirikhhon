export type Pin = {
    id: number
    label: string
    x: number
    y: number
}

export type WeaveLine = {
    start: Pin,
    end: Pin,
}

export type WeaveSolution = {
    lines: WeaveLine[]
    pins: Pin[]
}