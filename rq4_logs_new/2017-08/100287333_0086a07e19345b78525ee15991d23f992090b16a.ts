import { Component } from '@angular/core';
import { IonicPage, NavController, NavParams, Platform, AlertController, LoadingController, ToastController, ViewController } from 'ionic-angular';
import { ActionSheetController } from 'ionic-angular';
import { FileTransfer, FileTransferObject } from '@ionic-native/file-transfer';
import { Http } from '@angular/http';
import { FileOpener } from '@ionic-native/file-opener';
import 'rxjs/add/operator/map';
declare var cordova: any;
/**
 * Generated class for the KepwalPage page.
 *
 * See http://ionicframework.com/docs/components/#navigation for more info
 * on Ionic pages and navigation.
 */

@IonicPage()
@Component({
  selector: 'page-kepwal',
  templateUrl: 'kepwal.html',
})
export class KepwalPage {

  public datas: any;
  public filename: any;
  public dw: any;
  public item: any;
  public storageDirectory: string = '';
  public loading: any;
  public buffer: any;
  public link: any;
  public open: any;
  public filePath: any;
  public localPath: any;
  public posts: any;
  public searchQuery: string = '';
  public items: any;
  public dat: any;


  constructor(
    public navCtrl: NavController,
    public navParams: NavParams,
    public actionSheetCtrl: ActionSheetController,
    public platform: Platform,
    public alertCtrl: AlertController,
    public loadingCtrl: LoadingController,
    public toastCtrl: ToastController,
    public viewCtrl: ViewController,
    private transfer: FileTransfer,
    private http: Http,
    private fileOpener: FileOpener
  ) {
    this.posts = [];
    this.searchQuery = '';

    let loadingPopup = this.loadingCtrl.create({
      content: 'Loading posts...'
    });
    loadingPopup.present();
    this.http.get('http://jdih.awd.web.id/index.php/JsonKepwal/Data').map(res => res.json()).subscribe(data => {
      this.items = data['data'];
      loadingPopup.dismiss();
    }, (error) => {
      loadingPopup.dismiss();
      let alertFailure = this.alertCtrl.create({
        title: 'Failed Fetch Data!',
        message: 'Cek your Connection Internet or the server may be down',
        buttons: [
          {
            text: 'OK',
            role: 'cancel',
            handler: () => {
              this.viewCtrl.dismiss();
              console.log('Cancel clicked');
            }
          }
        ]
      });
      alertFailure.present();
    });
    this.initializeItems('');
  }

  ionViewDidLoad() {
    console.log('ionViewDidLoad KepwalPage');
  }

  public initializeItems(val) {
    if (val == '') {
      this.http.get('http://jdih.awd.web.id/index.php/JsonKepwal/Data').map(res => res.json()).subscribe(data => {
        this.posts = data['data'];
      }, (error) => {
        this.posts = [];
      });
    } else {
      this.posts = this.items;
    }
    this.posts = this.items;
    console.log(this.posts, 'json');
  }

  public getItems(ev: any) {
    let val = ev.target.value;
    this.initializeItems(val);
    console.log(val, 'Val Nya');
    if (val && val.trim() != '') {
      this.posts = this.items.filter((item) => {
        return item.tentang.toLowerCase().indexOf(val.toLowerCase()) > -1 || item.tahun_pembuatan.toLowerCase().indexOf(val.toLowerCase()) > -1;
      });
    }
  }

  public getDownload(datax) {
    let actionSheet = this.actionSheetCtrl.create({
      title: 'Choose your action',
      buttons: [
        {
          text: 'Download File and Read File',
          handler: () => {
            this.loader();
            const fileTransfer: FileTransferObject = this.transfer.create();
            this.link = datax.url_file;
            this.filename = datax.file_name_server;
            this.localPath = cordova.file.documentsDirectory + this.filename;

            fileTransfer.download(this.link, this.localPath)
              .then((entry) => {
                console.log('download sukses');
                this.loading.dismiss();

                this.open = entry;
                this.fileOpener.open(entry.toURL(), 'application/pdf')
                  .then(() => console.log('File is opened'))
                  .catch(e => console.log('Error openening file', e));

              }, (error) => {
                this.loading.dismiss();
                let alertFailure = this.alertCtrl.create({
                  title: 'Download Failed!',
                  subTitle: 'Cek your Connection Internet.',
                  buttons: ['Ok']
                });
                alertFailure.present();
              });
          }
        }, {
          text: 'Cancel',
          role: 'cancel',
          handler: () => {
            console.log('Cancel clicked');
          }
        }
      ]
    });
    actionSheet.present();
  }

  public load(): void {
    this.buffer = this.loadingCtrl.create({
      content: 'Please wait!'
    });
    this.buffer.present();
  }

  public loader(): void {
    this.loading = this.loadingCtrl.create({
      content: 'Please wait! Download File...'
    });
    this.loading.present();
  }

}