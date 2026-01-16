import { observable, action, computed, makeAutoObservable } from "mobx";
import { IWSQuery, WSQuery, IWSResult } from "../../../../../xcore/WSQuery";
import { WSocket } from "../../WSocket";
import { APP_STORAGE } from "../../AppStorage";
import { toJS } from "mobx";

export class ImportDevStorage {
  @observable modal: boolean = false;
  @observable jsonData: Array<any> = [];

  @observable text_button: string = "";

  @observable successfully_text = "";

  constructor() {
    makeAutoObservable(this);
  }

  @action setOpenModal(val: boolean) {
    this.modal = val;
  }
  @computed getOpenModal(): boolean {
    return this.modal;
  }

  @action setArrayJsonData(val: Array<any>) {
    this.jsonData = val;
  }
  @computed getArrayJsonData(): Array<any> {
    return this.jsonData;
  }

  @action setText_Button(val: string) {
    this.text_button = val;
  }
  @computed getText_Button(): string {
    return this.text_button;
  }

  @action setSuccessfully_text(val: string) {
    this.successfully_text = val;
  }
  @computed getSuccessfully_text(): string {
    return this.successfully_text;
  }

  async Uploadfile() {
    try {
      var m1: any;
      var array_senssors;
      for (var key in APP_STORAGE.importdevs.getArrayJsonData()) {
        m1 = APP_STORAGE.importdevs
          .getArrayJsonData()
          [key].slice(4, APP_STORAGE.importdevs.getArrayJsonData()[key].length);

        array_senssors = m1;

        var arrKeys = ["depth", "value"];

        var getData = (arrKeys: any[], array_senssors: any) => {
          return array_senssors.map((val: { toString: () => any }) => ({
            [arrKeys[0]]: val.toString(),
            [arrKeys[1]]: 1,
          }));
        };
        let arr = getData(arrKeys, array_senssors);

        var sess_code = APP_STORAGE.auth_form.getdt();
        var q: IWSQuery = new WSQuery("set_NewDevs");
        let lat: any;
        let lng: any;

        if (
          Number(APP_STORAGE.importdevs.getArrayJsonData()[key][2] !== 0) &&
          Number(APP_STORAGE.importdevs.getArrayJsonData()[key][3] !== 0)
        ) {
          let latnumber = APP_STORAGE.importdevs
            .getArrayJsonData()
            [key][2].toString()
            .replace(/[^\d\.,]/g, ""); //// только цифты
          let latchar = latnumber.replace(/,/g, "."); //// то
          if (latchar.match(/\./g).length > 1) {
            lat = latchar.substr(0, latchar.lastIndexOf("."));
          } else {
            lat = latchar;
          }

          let lngnumber = APP_STORAGE.importdevs
            .getArrayJsonData()
            [key][3].toString()
            .replace(/[^\d\.,]/g, ""); //// только цифты
          let lngchar = lngnumber.replace(/,/g, "."); //// то
          if (lngchar.match(/\./g).length > 1) {
            lng = lngchar.substr(0, latchar.lastIndexOf("."));
          } else {
            lng = lngchar;
          }
        } else if (
          Number(APP_STORAGE.importdevs.getArrayJsonData()[key][2] === 0) &&
          Number(APP_STORAGE.importdevs.getArrayJsonData()[key][3] === 0)
        ) {
          lat = "0.0";
          lng = "0.0";
        }

        q.args = {
          group_dev_id: APP_STORAGE.devs.getIdDevs(),
          number: APP_STORAGE.importdevs.getArrayJsonData()[key][0] || "",
          name: APP_STORAGE.importdevs.getArrayJsonData()[key][1] || "",
          latitude: lat,
          longitude: lng,
          sensors: '{"s":' + JSON.stringify(arr) + "}",
          deleted: false,
          info: "",
        };
        q.sess_code = sess_code;
        (await WSocket.get()).send(q);
      }
      setTimeout(() => {
        APP_STORAGE.devs_groups.get_DevsGroups(
          "sess_id",
          APP_STORAGE.auth_form.getdt()
        );
      }, 1000);
      APP_STORAGE.importdevs.setOpenModal(false);
    } catch (err) {
      this.setSuccessfully_text(
        "Ошибка! Убедитесь, что данные заполнены согласно шаблону"
      );
    }
  }
}