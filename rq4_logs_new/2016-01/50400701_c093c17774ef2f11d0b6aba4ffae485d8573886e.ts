export class GridOptions {
    constructor(
        public enableRowSelection: boolean,
        public columns: GridColumn[],
        public data?: GridData[]
    ){}
}
export class GridColumn {
    constructor(
        public displayName : string,
        public field : string
    ){}
}
export class GridData {

}