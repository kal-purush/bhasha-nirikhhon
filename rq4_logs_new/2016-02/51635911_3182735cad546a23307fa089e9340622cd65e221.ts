export function cr(colIndex: CalcJS.RefValue, rowIndex: CalcJS.RefValue, sheetIndex?: CalcJS.RefValue) : CalcJS.CellRef {
    return new CalcJS.CellRef(
                new CalcJS.RefUnit(CalcJS.RefKind.ById, colIndex), 
                new CalcJS.RefUnit(CalcJS.RefKind.ById, rowIndex), 
                new CalcJS.RefUnit(CalcJS.RefKind.ById, sheetIndex));
}

export function c$r(colIndex: CalcJS.RefValue, rowIndex: CalcJS.RefValue, sheetIndex?: CalcJS.RefValue) : CalcJS.CellRef {
    return new CalcJS.CellRef(
                new CalcJS.RefUnit(CalcJS.RefKind.ByIndex, colIndex), 
                new CalcJS.RefUnit(CalcJS.RefKind.ById, rowIndex), 
                new CalcJS.RefUnit(CalcJS.RefKind.ById, sheetIndex));
}

export function cr$(colIndex: CalcJS.RefValue, rowIndex: CalcJS.RefValue, sheetIndex?: CalcJS.RefValue) : CalcJS.CellRef {
    return new CalcJS.CellRef(
                new CalcJS.RefUnit(CalcJS.RefKind.ById, colIndex), 
                new CalcJS.RefUnit(CalcJS.RefKind.ByIndex, rowIndex), 
                new CalcJS.RefUnit(CalcJS.RefKind.ById, sheetIndex));
}

export function c$r$(colIndex: CalcJS.RefValue, rowIndex: CalcJS.RefValue, sheetIndex?: CalcJS.RefValue) : CalcJS.CellRef {
    return new CalcJS.CellRef(
                new CalcJS.RefUnit(CalcJS.RefKind.ByIndex, colIndex), 
                new CalcJS.RefUnit(CalcJS.RefKind.ByIndex, rowIndex), 
                new CalcJS.RefUnit(CalcJS.RefKind.ById, sheetIndex));
}


namespace CalcJS {
    export class CellRef {
        public constructor(colRef: RefUnit, rowRef: RefUnit, sheetRef?: RefUnit) {
            this.colRef = colRef;
            this.rowRef = rowRef;
            this.sheetRef = sheetRef;
        }
        
        public colRef: RefUnit;
        public rowRef: RefUnit;
        public sheetRef: RefUnit;
    }
    
    
    export class RefUnit {
        public constructor (kind: RefKind, value: RefValue) {
            this.kind = kind;
            this.value = value;
        }
        
        public kind: RefKind;
        public value: RefValue;
    }
    
    export type RefValue = number;
    
    export const enum RefKind {
        ById = 0,
        ByIndex = 1,
    }
}