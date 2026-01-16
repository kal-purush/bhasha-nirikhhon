import { Component } from '@angular/core';
import { Camera, CameraOptions } from '@ionic-native/camera';
import { IonicPage, NavController, NavParams, ViewController, AlertController } from 'ionic-angular';
import { FileTransfer, FileUploadOptions, FileTransferObject } from '@ionic-native/file-transfer';
import { File } from '@ionic-native/file';

@IonicPage()
@Component({
  selector: 'page-activity-item-edit',
  templateUrl: 'activity-item-edit.html',
})
// export class ActivityItemEditPage {
//   @ViewChild('fileInputStorePics') fileInputStorePics;
//   @ViewChild('fileInput') fileInput;
//   isReadyToSave = false;

//   activity: any;
//   selectedPage: any;
//   isReadyToSubmitPreapre: boolean;
//   isReadyToSubmitResult: boolean;
//   form_prepare: FormGroup;
//   form_result: FormGroup;
//   form: FormGroup;

//   constructor(public navCtrl: NavController, public navParams: NavParams, public viewCtrl: ViewController, formBuilder: FormBuilder, public camera: Camera) {
//     this.isReadyToSubmitPreapre = false;
//     this.isReadyToSubmitResult = false;
//     this.activity = this.navParams.get("item");
//     // demo form
//     this.form = formBuilder.group({
//       profilePic: [''],
//       name: ['', Validators.required],
//       about: ['']
//     });

//     // Watch the form for changes, and
//     this.form.valueChanges.subscribe((v) => {
//       this.isReadyToSave = this.form.valid;
//     });

//     // 活动准备Form
//     this.form_prepare = formBuilder.group({
//       "storePics": ['', Validators.required],
//       "shelvesPics": ['', Validators.required],
//       "storeShowPics": ['', Validators.required]
//     });

//     // 结果上传Form
//     this.form_result = formBuilder.group({
//       "join": ['', Validators.required],
//       "attend": ['', Validators.required],
//       "live": ['', Validators.required],
//       "playersPics": ['', Validators.required],
//       "spectatorPics": ['', Validators.required],
//       "advertisePics": ['', Validators.required],
//       "liverPics": ['', Validators.required],
//       "supporterPics": ['', Validators.required]
//     });

//     this.form_prepare.valueChanges.subscribe((v) => {
//       this.isReadyToSubmitPreapre = this.form_prepare.valid;
//     });

//     this.form_result.valueChanges.subscribe((v) => {
//       this.isReadyToSubmitResult = this.form_result.valid;
//     });
//   }

//   ionViewDidLoad() {
//     console.log('ionViewDidLoad ActivityItemEditPage');
//   }

//   getPicture() {
//     if (Camera['installed']()) {
//       this.camera.getPicture({
//         destinationType: this.camera.DestinationType.DATA_URL,
//         targetWidth: 96,
//         targetHeight: 96
//       }).then((data) => {
//         console.log(data);
//         this.form_prepare.patchValue({ 'storePics': 'data:image/jpg;base64,' + data });
//       }, (err) => {
//         alert('Unable to take photo');
//       })
//     } else {
//       // this.fileInputStorePics.nativeElement.click();
//       this.fileInput.nativeElement.click();
//     }
//   }

//   processWebImage(event) {
//     console.log(event.target.value);
//     let reader = new FileReader();
//     reader.onload = (readerEvent) => {
//       let imageData = (readerEvent.target as any).result;
//       console.log(imageData);
//       this.form_prepare.patchValue({ 'storePics': imageData });
//     };

//     reader.readAsDataURL(event.target.storePics[0]);
//   }

//   getProfileImageStyle() {
//     return 'url(' + this.form_prepare.controls['storePics'].value + ')'
//   }

// }


export class ActivityItemEditPage {

  activity: any;
  isReadyToSave: boolean;

  item: any;
  selectedPage: any;

  public base64Image: string;
  // 活动准备照片
  storePics: any;
  shelvesPics: any;
  storeShowPics: any;

  constructor(public navCtrl: NavController, public navParams: NavParams, public viewCtrl: ViewController, public camera: Camera, private alertCtrl: AlertController) {
    this.activity = this.navParams.get("item");
    this.selectedPage = "prepare";
    this.storePics = ['https://ionicframework.com/dist/preview-app/www/assets/img/nin-live.png', 'https://ionicframework.com/dist/preview-app/www/assets/img/badu-live.png'];
    this.shelvesPics = ['https://ionicframework.com/dist/preview-app/www/assets/img/nin-live.png', 'https://ionicframework.com/dist/preview-app/www/assets/img/badu-live.png'];
    this.storeShowPics = ['https://ionicframework.com/dist/preview-app/www/assets/img/nin-live.png', 'https://ionicframework.com/dist/preview-app/www/assets/img/badu-live.png'];
  }

  ionViewDidLoad() {

  }

  // 获取照片
  getImage(imageArray) {
    const options: CameraOptions = {
      quality: 50,
      destinationType: this.camera.DestinationType.FILE_URI,
      encodingType: this.camera.EncodingType.JPEG,
      mediaType: this.camera.MediaType.PICTURE
    }
    // 打开照相机
    if (Camera['installed']()) {
      this.camera.getPicture(options).then((imageData) => {
        this.base64Image = "file://" + imageData;
        switch (imageArray) {
          case "storePics":
            console.log("storePics");
            this.storePics.push(this.base64Image);
            this.storePics.reverse();
            break;
          case "shelvesPics":
            console.log("shelvesPics");
            this.shelvesPics.push(this.base64Image);
            this.shelvesPics.reverse();
            break;
          case "storeShowPics":
            console.log("storeShowPics");
            this.storeShowPics.splice(this.base64Image);
            this.storeShowPics.reverse();
            break;
        }
      }, (err) => {
        alert('Unable to take photo');
      })
    } else {
      // 打开照相机
      console.log("打开本地文件");
      // this.fileInput.nativeElement.click();
    }
  }

  // 删除照片
  doDelete(imageArray, index) {
    let confirm = this
      .alertCtrl
      .create({
        title: '确认要删除吗？',
        message: '',
        buttons: [
          {
            text: '否',
            handler: () => {
              console.log('Disagree clicked');
            }
          }, {
            text: '是',
            handler: () => {
              switch (imageArray) {
                case "storePics":
                  console.log("storePics");
                  this.storePics.splice(index, 1);
                  break;
                case "shelvesPics":
                  console.log("shelvesPics");
                  this.shelvesPics.splice(index, 1);
                  break;
                case "storeShowPics":
                  console.log("storeShowPics");
                  this.storeShowPics.splice(index, 1);
                  break;
              }
            }
          }
        ]
      });
    confirm.present();
  }
}