import { DictionnaireService } from "./../services/dictionnaireService";
import { ILigneDictionnaire } from "./../modeles/ligneDictionnaireModel";
import { AccessService } from "./../services/accessService";
import { ENVIRONNEMENT } from "./../../constantes/constantesUtilis";
import { AlertController } from "ionic-angular/components/alert/alert-controller";
import { IUtilisateur } from "./../modeles/utilisateurModel";
import { Component } from "@angular/core";
import { NavController, NavParams } from "ionic-angular";
import { MotsAValiderPage } from "./mots-a-valider/mots-a-valider";

@Component({
  selector: "page-gestion",
  templateUrl: "gestion.html"
})
export class GestionPage {
  user: IUtilisateur;
  admin: boolean = false;
  motsAvalider: ILigneDictionnaire[];

  constructor(
    public navCtrl: NavController,
    public navParams: NavParams,
    private alertCtrl: AlertController,
    private accessService: AccessService,
    private dictionnaireSrv: DictionnaireService
  ) {
    this.user = this.navParams.data;
    this.admin = this.accessService.checkAdmin(this.user.role);
  }

  validerDesMots() {
    this.choixDialect();
  }

  choixDialect() {
    let alert = this.alertCtrl.create({
      title: "Dialecte à valider",
      message: "Veuillez choisir votre langue de compétence",
      inputs: ENVIRONNEMENT.inputsDialect,
      buttons: [
        {
          text: "Annuler",
          handler: () => {}
        },
        {
          text: "Ok",
          handler: value => {
            this.dictionnaireSrv
              .listerMotsAValider(value, this.user.token)
              .subscribe((result: ILigneDictionnaire[]) => {
                this.motsAvalider = result;
              });
            this.navCtrl.push(MotsAValiderPage, {
              dialect: value,
              user: this.user,
              mots: this.motsAvalider
            });
          }
        }
      ]
    });
    alert.present();
  }

  gererLesUsers() {}
}