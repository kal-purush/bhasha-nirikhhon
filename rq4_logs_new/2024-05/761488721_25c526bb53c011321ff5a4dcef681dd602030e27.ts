import { z } from "zod";

export class postValidator {
    private postValByField: { [key: string]: z.ZodString }

    constructor() {
        this.postValByField = {
            //title: z.string().regex(/.+/, 'Preencha esse campo!')
            description: z.string().regex(/.+/, 'Preencha esse campo!')
        }
    }

    private async validateField(validator: z.ZodString, value: string): Promise<string> {
        return await validator.parseAsync(value);
    }

    async valByField(field: string, value: string): Promise<{valido: boolean, value?: string, erro?: string}> {
        if(field === 'description' /* | field === 'title' */){
            return { valido: true, value: 'ok', erro: undefined };
        }
        
        const validator = this.postValByField[field]; // Obtém o esquema de validação para o field
        
        if (!validator) {
            return { valido: false, erro: 'Esquema de validação não encontrado para o campo especificado.' };
        }

        try {
            const valueValidado = await this.validateField(validator, value);;
            return { valido: true, value: String(valueValidado), erro: undefined }; // Retorno válido    
        } catch (error) {
            if (error instanceof z.ZodError) {
                const mensagemErro = error.errors[0].message; 
                return { valido: false, erro: mensagemErro }; // Retorno inválido com mensagem de erro do Zod
            } else {
                return { valido: false, erro: 'Erro desconhecido ao validar o texto' };
            }
        }   
    }
}