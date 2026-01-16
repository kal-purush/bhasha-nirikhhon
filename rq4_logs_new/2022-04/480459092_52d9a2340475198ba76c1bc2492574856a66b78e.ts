export function signedCoef(coef: number, op: string) {
    if ((op === "-" && coef < 0) || (op === "+" && coef >= 0))
        return `+ ${coef}`;
    if (op === "-" && coef === 0) return "- 0";
    return `${coef}`;
}

export function prettifyCoeffs(a: number, b: number, c: number) {
    let output = "";
    switch (a) {
        case 0:
            break;
        case 1:
            output += "x^2"
            break;
        case -1:
            output += "-x^2"
            break;
        default:
            output += `${a}x^2`
            break;
    }
    switch (b) {
        case 0:
            output += ''
            break;
        case 1:
            output += "+x"
            break;
        case -1:
            output += "-x"
            break;
        default:
            output += `${signedCoef(b, '+')}x`
            break;
    }
    switch (c) {
        case 0:
            output += ''
            break;
        default:
            output += `${signedCoef(c, '+')}`
            break;
    }
    return output;
}