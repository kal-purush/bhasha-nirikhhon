import { Usuario } from '../models/Usuario';
import { Docente } from './../models/Docente';
import {BaseDatabase} from "./BaseDatabase";

export class DocenteData extends BaseDatabase{
    TABLE_NAME = "Docente"
    TABLE_TURMA = "Turma"

    public async buscaDocente(email:string){
        const result = await BaseDatabase.connection(this.TABLE_NAME)
        .select()
        .where({email})
        
        return result[0]
    }

    public async buscaIdTurma(id:string) {
        const result = await BaseDatabase.connection(this.TABLE_TURMA)
        .select()
        .where({id})

        return result
    }

    public async criarDocente (docente: Docente) {
        await BaseDatabase.connection(this.TABLE_NAME)
        .insert({
            id:docente.getId(),
            nome:docente.getName(),
            email:docente.getEmail(),
            data_nasc:docente.getDataNasc(),
            turma_id:docente.getTurmaId()})
        .into(this.TABLE_NAME)
    }

    public async getAllDocentes() {
        const result = await BaseDatabase.connection(this.TABLE_NAME)
        .select()
        return result
    }

    public async getDocenteId(id:string){
        const result = await BaseDatabase.connection(this.TABLE_NAME)
        .select()
        .where({id})
        return result
    }

    public async mudarDocenteTurma(id:string,turma_id:string){
        await BaseDatabase.connection(this.TABLE_NAME)
        .update({
            turma_id:turma_id
        })
        .into(this.TABLE_NAME)
        .where({id:id})

        return `turma alterada com sucesso`

    }
}