import {Component} from '@angular/core';
import {IonicPage, NavController, NavParams, Platform} from 'ionic-angular';
import {QRScanner, QRScannerStatus} from "@ionic-native/qr-scanner";
import {WatcherProvider} from "../../providers/watcher/watcher";

@IonicPage()
@Component({
  selector: 'page-qr-scanner',
  templateUrl: 'qr-scanner.html',
})
export class QrScannerPage {

  public isNative: boolean = false;
  private scanSub: any;

  constructor(public navCtrl: NavController, public navParams: NavParams, public platform: Platform,
              public qrScanner: QRScanner) {
  }

  ionViewDidLoad() {
    this.isNative = false;
    this.platform.ready().then((readySource) => {
      console.log('Platform ready from', readySource);
      if (readySource == "cordova") {
        this.isNative = true;
        this.scanQr();
      } else {
        this.isNative = false;
      }
    });
  }

  scanQr(): Promise<QRScannerStatus> {
    if (this.isNative) {
      this.qrScanner.prepare()
        .then((status: QRScannerStatus) => {
          console.log("scanQr prepare");
          if (status.authorized) {
            // camera permission was granted

            // start scanning
            this.scanSub = this.qrScanner.scan().subscribe((input: string) => {

              this.qrScanner.hide(); // hide camera preview
              this.scanSub.unsubscribe(); // stop scanning
              this.navParams.get('resolve')(input);
              this.navCtrl.pop();
            });

            // show camera preview
            return this.qrScanner.show();

            // wait for user to scan something, then the observable callback will be called

          } else if (status.denied) {
            alert("camera permission was permanently denied");
            alert("you must use QRScanner.openSettings() method to guide the user to the settings page");
            alert("then they can grant the permission from there");
            //TODO: refactor this
          } else {
            alert("permission was denied, but not permanently");  //TODO: refactor this
            // permission was denied, but not permanently. You can ask for permission again at a later time.
          }
        })
        .catch((e: any) => console.log('Error is', e));


    } else {
      return null
    }
  }

  ionViewWillLeave() {
    if (this.isNative) {
      this.qrScanner.hide(); // hide camera preview
      if (this.scanSub != null) {
        this.scanSub.unsubscribe(); // stop scanning
      }
    }
  }
}