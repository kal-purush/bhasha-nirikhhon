import ICategorieQuestion from '@/interfaces/ICategorieQuestion';

export default class Metier {
    public idCategorieQuestion: string;
    public libelle : string;

    constructor(idCategorieQuestion: string, libelle : string){
        this.idCategorieQuestion = idCategorieQuestion;
        this.libelle  = libelle ;
    }

    public toJSON(): ICategorieQuestion{
        return {
            "idCategorieQuestion": this.idCategorieQuestion,
            "libelle": this. libelle
        }
    }
}