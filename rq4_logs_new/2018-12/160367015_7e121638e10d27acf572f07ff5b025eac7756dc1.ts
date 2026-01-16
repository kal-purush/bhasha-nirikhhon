export interface InputJson {
    fields: Array<string>,
    columns: Array<{ name: string, sign: number, compare_param: number }>,
    initial_data: Array<Array<number>>
}