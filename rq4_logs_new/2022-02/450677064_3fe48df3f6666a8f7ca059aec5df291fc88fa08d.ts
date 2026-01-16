import ICategorieQuestion from '../interfaces/ICategorieQuestion';
export default class CategorieQuestion {
    public idCategoriesQuestion: string;
    public libelle: string;

    constructor(idCategoriesQuestion: string, libelle: string){
        this.idCategoriesQuestion = idCategoriesQuestion;
        this.libelle = libelle;
    }

    public toJSON(): ICategorieQuestion{
        return {
            "idCategoriesQuestion": this.idCategoriesQuestion,
            "libelle": this.libelle
        }
    }
}