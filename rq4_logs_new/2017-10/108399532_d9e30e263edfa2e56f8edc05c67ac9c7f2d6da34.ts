import { ValidatorFn, AbstractControl, Validators } from "@angular/forms";


export function minimum(value:number):ValidatorFn{
    return (control:AbstractControl):{[key:string]:any}=>{
        if(Validators.required(control)){
            return null;
        }
        if(control.value<value){
            return {'minimum':{value:value, actual:control.value}}
        }
    }
}

export function maximum(value:number):ValidatorFn{
    return (control:AbstractControl):{[key:string]:any}=>{
        if(Validators.required(control)){
            return null;
        }
        if(control.value>value){
            return {'maximum':{value:value, actual:control.value}}
        }
    }
}