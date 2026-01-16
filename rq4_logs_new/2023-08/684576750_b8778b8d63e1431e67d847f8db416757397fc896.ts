// Ajouter votre code ici

enum Var{
    //% block="rax"
    Rax,
    //% block="rbx"
    Rbx,
    //% block="eci"
    Eci,
}

enum what {
    //% block="+"
    Add,
    //% block="-"
    Sub,
    //% block="x"
    Mul,
    //% block="/"
    Div,
}

//% icon="\uf26c" color=#F80800
namespace assambleur{
    let rax = 0
    let rbx = 0
    let eci = 0
    //%block
    export function Op(reg:Var,op:what,num:number){
        if(op == 0){
            if (reg == 0) {
                rax += num
            } else if (reg == 1) {
                rbx += num
            } else {
                eci += num
            }
        }else if(op == 1){
            if (reg == 0) {
                rax -= num
            } else if (reg == 1) {
                rbx -= num
            } else {
                eci -= num
            }
        }else if (op == 2){
            if (reg == 0) {
                rax *= num
            } else if (reg == 1) {
                rbx *= num
            } else {
                eci *= num
            }
        }else{
            if (reg == 0) {
                rax /= num
            } else if (reg == 1) {
                rbx /= num
            } else {
                eci /= num
            }
        }
    }
    //% block
    export function get(reg: Var): number{
        if (reg == 0){
            return rax
        }else if (reg == 1) {
            return rbx
        } else {
            return eci
        }
    }
    //% block
    export function cpx(reg: number,reg2:number): boolean{
       return (reg == reg2)
    }
}