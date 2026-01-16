export type OpenElement = {
    id: string
    count: number
 }


export interface DOMModelInitialState {
    openElements: OpenElement[]
}