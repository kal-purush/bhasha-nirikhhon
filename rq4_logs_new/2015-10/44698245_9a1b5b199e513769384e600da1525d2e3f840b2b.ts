class Int {
    static div(n1:number, n2:number):number {
        return (n1 - (n1 % n2)) / n2;
    }

    static padLeft(value:number, digits:number, char?:string):string {
        return new Array(digits - String(value).length + 1).join(char || '0') + value;
    }
}
export = Int;