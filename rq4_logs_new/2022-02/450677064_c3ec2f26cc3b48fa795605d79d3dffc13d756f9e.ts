import IPreconisationCategorieQuestion from "../interfaces/IPreconisationCategorieQuestion";
export default class PreconisationCategorieQuestion {

  public idPreconisationCategoriesQuestion: string;
  public viewIfPourcentageScoreLessThan: number;
  public Contenue: string;

  constructor(idPreconisationCategoriesQuestion: string, viewIfPourcentageScoreLessThan: number, Contenue: string ){
      this.idPreconisationCategoriesQuestion =idPreconisationCategoriesQuestion;
      this.viewIfPourcentageScoreLessThan = viewIfPourcentageScoreLessThan;
      this.Contenue = Contenue;
  }

  public toJSON(): IPreconisationCategorieQuestion{
      return {
        "idPreconisationCategoriesQuestion": this.idPreconisationCategoriesQuestion,
        "viewIfPourcentageScoreLessThan": this.viewIfPourcentageScoreLessThan,
        "Contenue": this.Contenue
      }
  }
}