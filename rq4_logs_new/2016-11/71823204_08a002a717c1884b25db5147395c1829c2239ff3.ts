let a: (string | number )[] = [1, '2', '3', 5, 6];

// for (let c of a) {
//     let res: number = 0;
//     if(typeof c === 'string' ){
//         res += parseInt(c, 10);
//         // cтрока
//     }
//     if(typeof c === 'number' ){
//         res += c;
//         // число
//     }
//     // строка или число
// }


// for (let i = 0; i<a.length;i++) {
//     let res: number = 0;
//     let c = a[i];
//     if(typeof c === 'string' ){
//         res += parseInt(c, 10);
//         // cтрока
//     }
//     if(typeof c === 'number' ){
//         res += c;
//         // число
//     }
//     // строка или число
// }

/// <reference path="interfaces.ts" />


// namespace Validation{
//
//     import IValidator = Interfaces.IValidator;
//
//     export class NameValidator implements IValidator {
//         public isValid(name: string): boolean {
//             return /^([aA-zZ\-]+)$/.test(name);
//         }
//     }
//
//     export class PhoneValidator implements IValidator {
//         public isValid(phone: string): boolean {
//             return /^093\d{7}$/.test(phone);
//         }
//     }
// }
//
//
// let nameValidator = new Validation.NameValidator();
// let phoneValidator = new Validation.PhoneValidator();
//
// console.log(nameValidator.isValid('Igor'));
// console.log(phoneValidator.isValid('0934595959'));


// declare type ClassDecorator = <TFunction extends Function>(target: TFunction) => TFunction | void;
// declare type PropertyDecorator = (target: Object, propertyKey: string | symbol) => void;
// declare type MethodDecorator = <T>(target: Object, propertyKey: string | symbol, descriptor: TypedPropertyDescriptor<T>) => TypedPropertyDescriptor<T> | void;
// declare type ParameterDecorator = (target: Object, propertyKey: string | symbol, parameterIndex: number) => void;


// class MathLib {
//     @logMethod()
//     public areaOfCircle(r: number): number {
//         return Math.PI * r ** 2;
//     }
// }
//
// function logMethod(target: any, key: string, desc: any): any {
//     return {
//         value: (...args: any[])=> {
//             let b = args.map((a: any)=>JSON.stringify(a)).join();
//             let result = desc.value(...args);
//             let r = JSON.stringify(result);
//             console.log(`Call: ${key}(${b}) => ${r}`);
//             return result;
//         }
//     }
// }
//
//
// let math = new MathLib()
//
// math.areaOfCircle(3);

// class Account {
//     @logProperty
//     public firstName: string;
//     public lastName: string;
//
//     public constructor(firstName: string, lastName: string) {
//         this.firstName = firstName;
//         this.lastName = lastName;
//     }
// }
//
// function logProperty(target: any, key: string): void {
//     let _val = target[key];
//
//     let getter = (): typeof _val=> {
//         console.log(`Get: ${key} => ${_val}`);
//     }
//     let setter = (newValue:any): void=> {
//         console.log(`Set: ${key} => ${newValue}`);
//         _val = newValue
//     }
//
//     Object.defineProperty(target,key,{
//         get:getter,
//         set:setter,
//         enumerable:true,
//         configurable:true
//     })
// }
//
// let me = new Account('Igor','Nepipenko');
// let new_name = me.firstName;
// me.firstName = 'Vova';

// @logClass
// class Account {
//     public firstName: string;
//     public lastName: string;
//
//     public constructor(firstName: string, lastName: string) {
//         this.firstName = firstName;
//         this.lastName = lastName;
//     }
// }
// function logClass(target:any):any{
//     return ()=>{
//         console.log(`New instance of ${target.name}`);
//         return target;
//     }
// }
//
// let firstPersone = new Account('Lena','Belova');
// let secondPersone = new Account('Vlad','Yama');


class Account {
    public firstName: string;
    public lastName: string;

    public constructor(firstName: string, lastName: string) {
        this.firstName = firstName;
        this.lastName = lastName;
    }


    @readMetadata
    public sayMessage(@setMetadata msg:string):string{
        return `${this.firstName} ${this.lastName} :  ${msg}`
    }
}

function setMetadata(target:any,key:string,index:number):void{
    let metadataKey = `___log_${key}_parameters`;
    if(Array.isArray(target[metadataKey])){
        target[metadataKey].push(index);
        return;
    }
    target[metadataKey] = [index];
}

function readMetadata(target:any, key:string, desc:any):any{
    let metadataKey = `___log_${key}_parameters`;
    let indices = target[metadataKey];
    let originDesc = desc.value;
    desc.value =(...args:any[]):any=>{
        console.log(`${key} arg[${indices}]: ${args[indices]}`);
        return originDesc(...args)
    };
    return desc
}


let persone = new Account('Igor','Nepipenko');
persone.sayMessage('TypeScript is the best');
persone.sayMessage('Angular2 is awesome ');