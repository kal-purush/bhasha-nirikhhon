import { Component, Input } from "@angular/core";
import { askrecallModel } from "../../../models/askrecall.model";
import { NavParams, NavController } from "ionic-angular";
import { SuratProvider } from "../../../providers/surat/surat";
import { ToastHelper } from "../../../helpers/toast-helper";
import { DatepickerProvider } from "../../../providers/datepicker/datepicker";
import { MomentHelper } from "../../../helpers/moment-helper";
import { LoaderHelper } from "../../../helpers/loader-helper";

@Component({
  selector: "askrecall",
  templateUrl: "askrecall.html"
})
export class Askrecall {
  @Input() type: string;
  detail: any = "";
  errors: any = "";

  data: askrecallModel = {
    alasan: ""
  };
  constructor(
    private suratProvider: SuratProvider,
    private toastHelper: ToastHelper,
    private navParam: NavParams,
    private datepicker: DatepickerProvider,
    private momentHelper: MomentHelper,
    private navCtrl: NavController,
    private loaderHelper: LoaderHelper
  ) {
    this.detail = this.navParam.get("detailNaskah");
  }

  askrecall(data: askrecallModel) {
    this.loaderHelper.createLoader();
    this.suratProvider.simpanAskrecall(this.detail, data).subscribe(
      res => {
        this.loaderHelper.dismiss();
        this.toastHelper.present(res.messages);
        this.navCtrl.pop();
      },
      err => {
        this.loaderHelper.dismiss();
        if (err.errors) {
          this.errors = err.errors;
        } else {
          this.toastHelper.present(err);
        }
      }
    );
  }

  
}