import { AbstractControl, ValidatorFn } from "@angular/forms";

export function confirmarSenharIguais(senhaField: string, confirmarField: string): ValidatorFn{
    return (formGroup: AbstractControl): {[key: string]: any} | null => {
        const senha =  formGroup.get(senhaField);
        const confirmarSenha =  formGroup.get(confirmarField);

        if(senha && confirmarSenha && senha.value !== confirmarSenha.value){
            confirmarSenha.setErrors({ senhasDiferentes: true});

            return {senhasDiferentes: true};
        } else {
            if(confirmarSenha?.hasError('senhasDiferentes')) {
                confirmarSenha.setErrors(null);
            }

            return null;
        }
    }
}