import { Component } from '@angular/core';
import { IonicPage, NavController, NavParams, ToastController, AlertController,  LoadingController, Loading, ActionSheetController, Platform } from 'ionic-angular';
import { AuthServiceProvider, ResponseMessage } from '../../providers';
import { FormControl, FormBuilder, Validators, FormGroup, AbstractControl, FormArray } from '@angular/forms';
import { Geolocation } from '@ionic-native/geolocation';
import { NativeGeocoder,  NativeGeocoderReverseResult } from '@ionic-native/native-geocoder';
import { Camera, CameraOptions } from '@ionic-native/camera';
import { FileTransfer, FileUploadOptions, FileTransferObject } from '@ionic-native/file-transfer';
import { FilePath } from '@ionic-native/file-path';
import { File } from '@ionic-native/file';
import * as _ from 'lodash';

/**
 * Generated class for the EditgroupPage page.
 *
 * See https://ionicframework.com/docs/components/#navigation for more info on
 * Ionic pages and navigation.
 */
declare var cordova: any;
@IonicPage()
@Component({
  selector: 'page-editgroup',
  templateUrl: 'editgroup.html',
})
export class EditgroupPage {
  public groupId:any;
  public userData:any;
  public loading: Loading;
  public getimageURI:any;
  public groupDetails:any;
  public groupTag = [];
  private form: FormGroup;

  public userSettings: any = {
    inputPlaceholderText: 'Location'
  };
  public locJsonData:any;
  public interestList = [];
  public InterestDropdownList = [];
  public getApiUrl:any;
  public mySelectedIntList = [];
  public myInputStr:string = '';
  public lastImage: string = null;

  constructor(
    public navCtrl: NavController, 
    public dataService: AuthServiceProvider,
    public loadingCtrl: LoadingController,
    public platform: Platform,
    public toastCtrl: ToastController,
    public alertCtrl: AlertController,
    public jsonErrMsg: ResponseMessage,
    private camera: Camera,
    private actionSheetCtrl: ActionSheetController,
    private transfer: FileTransfer,
    private file: File, 
    private filePath: FilePath,
    public navParams: NavParams
  ) {
    this.groupId = this.navParams.get('group_id');
    const loguserDet = JSON.parse(localStorage.getItem('userPrfDet'));
    this.userData=loguserDet;
    this.getGroupDetails();
    this.form = new FormGroup({
      name: new FormControl('', [Validators.required, Validators.pattern('[a-zA-Z ]*')]),
      location: new FormControl('', [Validators.required]),
      loc_lat: new FormControl(''),
      loc_long: new FormControl(''),
      description: new FormControl(''),
      selectedInterest: new FormControl('')
    });
    this.getimageURI= this.dataService.apiImgUrl();
    this.getApiUrl= this.dataService.apiUrlFun();
  }

  ionViewDidLoad() {
    this.getGroupTagList();
    
  }

  public getInterestList(){
    let filterUserData = '{"where":{"is_active":true, "or":[{"is_hidden":0},{"is_hidden":null}]}}';
    this.dataService.getData('interests?filter=' + filterUserData).then((result:any) => {
      if (result.length > 0) {
        result.forEach((color: { name: string, id: number, description: string }) => {
          let checkInt = _.includes(this.mySelectedIntList, color.id);
          let checkedVal:boolean = false;
          if(checkInt){
            checkedVal= true;
          }
          this.InterestDropdownList.push({
            id: color.id,
            name: color.name,
            description: color.description,
            checked:checkedVal
          });
        });
        this.interestList = this.InterestDropdownList;
      }
    }, (err) => {
      
    });
  }

  getGroupDetails(){
    this.loading = this.loadingCtrl.create({
      content: 'Please wait...',
    });
    this.loading.present();
    this.dataService.getData('UserGroups/'+this.groupId).then((res:any)=>{
        
        let grpImg = '';
        if(res.image != null){
          grpImg = this.getimageURI+'group/'+res.image;
        }else{
          grpImg = './assets/img/default.jpeg';
        }
        res.image_url=grpImg;
        this.groupDetails= res;
        this.form.controls['name'].setValue(res.name);
        this.form.controls['loc_lat'].setValue(res.loc_lat);
        this.form.controls['loc_long'].setValue(res.loc_long);
        this.form.controls['location'].setValue(res.location);
        this.form.controls['description'].setValue(res.description);
        
        if(res.location!=''){
          this.userSettings['inputString']=res.location;
          this.userSettings = Object.assign({},this.userSettings);
        }
        //console.log(this.groupDetails);
        this.loading.dismissAll()
    },err=>{
      this.loading.dismissAll()
    })
  }

  getGroupTagList(){
    let filterData = '{"where":{"group_id":'+this.groupId+'}, "include":["interest"], "order" : "id desc"}';
    this.dataService.getData('GroupInterests?filter='+filterData).then((result:any) => {
      this.groupTag = result;
      if(this.groupTag.length>0){
        this.groupTag.forEach(element => {
          if(element.interestId!=''){
            this.mySelectedIntList.push(element.interestId);
          }
        });
      }
      this.getInterestList();
    }, (err) => {
      
    }); 
  }

  public searchItems(ev: any) {
    // set val to the value of the searchbar
    const val = ev.target.value;
    // if the value is an empty string don't filter the items
    if (val && val.trim() != '') {
      this.myInputStr =val;
      let trimVal =val.trim();
      this.InterestDropdownList=this.searchPipe(this.interestList, trimVal);
    }else{
      this.InterestDropdownList = this.interestList;
      this.myInputStr = '';
    }
    //console.log(this.interestNameTest);
  }

  public searchPipe(items, sdata){
    const /** @type {?} */ toCompare = sdata.toLowerCase();
    return items.filter(function (item) {
        for (let /** @type {?} */ property in item) {
          //console.log(item);
            if (item[property] === null) {
                continue;
            }
            if (item[property].toString().toLowerCase().includes(toCompare)) {
                return true;
            }
        }
        return false;
    });
  }

  public autoCompleteCallback1(selectedData:any) {
    this.locJsonData=JSON.parse(JSON.stringify(selectedData));
    if(this.locJsonData.data.formatted_address!=''){
      this.form.controls['location'].setValue(this.locJsonData.data.formatted_address);
      this.form.controls['loc_lat'].setValue(this.locJsonData.data.geometry.location.lat);
      this.form.controls['loc_long'].setValue(this.locJsonData.data.geometry.location.lng);
    }
  }

  private presentToast(text) {
    let toast = this.toastCtrl.create({
      message: text,
      duration: 3000,
      position: 'top'
    });
    toast.present();
  }

  public uploadImage() {
    this.loading = this.loadingCtrl.create({
      content: 'Please wait...',
    });
    this.loading.present();

    let uId = this.groupId;
    // Destination URL
    var url = this.getApiUrl+'UserGroups/uploadimg/'+uId;
    // File for Upload
    var targetPath = this.pathForImage(this.lastImage);
   
    // File name only
    var filename = this.lastImage;
   //console.log(filename);
    var options = {
      fileKey: "photo",
      photo: filename,
      chunkedMode: false,
      mimeType: "multipart/form-data",
      params : {
      'image':filename,
      'upload_id':uId
      }
     // params : {'fileName': filename}
    };
    const fileTransfer:FileTransferObject = this.transfer.create();
    // Use the FileTransfer to upload the image
    fileTransfer.upload(targetPath, url, options).then(data => {
      this.presentToast('Group Image Uploaded Successfully.');
      this.loading.dismissAll();
    }, err => {
      this.loading.dismissAll();
      this.presentToast('Error while uploading file.');
    });
  }

  public pathForImage(img) {
    if (img === null) {
      return '';
    } else {
      return cordova.file.dataDirectory + img;
    }
  }

  presentActionSheet() {
    let actionSheet = this.actionSheetCtrl.create({
      enableBackdropDismiss: true,
      buttons: [
        {
          text: 'Take a picture',
          icon: 'camera',
          handler: () => {
            this.uploadFromCamera(this.camera.PictureSourceType.CAMERA);
          }
        }, {
          text: 'From gallery',
          icon: 'images',
          handler: () => {
            this.uploadFromCamera(this.camera.PictureSourceType.PHOTOLIBRARY);
          }
        }
      ]
    });
    actionSheet.present();
  }

  uploadFromCamera(sourceType){
    var options = {
      quality: 100,
      sourceType: sourceType,
      saveToPhotoAlbum: false,
      correctOrientation: true
    };
   
    // Get the data of an image
    this.camera.getPicture(options).then((imagePath) => {
      // Special handling for Android library
      if (this.platform.is('android') && sourceType === this.camera.PictureSourceType.PHOTOLIBRARY) {
        this.filePath.resolveNativePath(imagePath)
          .then(filePath => {
            let correctPath = filePath.substr(0, filePath.lastIndexOf('/') + 1);
            let currentName = imagePath.substring(imagePath.lastIndexOf('/') + 1, imagePath.lastIndexOf('?'));
            this.copyFileToLocalDir(correctPath, currentName, this.createFileName(currentName));
          });
      } else {
        var currentName = imagePath.substr(imagePath.lastIndexOf('/') + 1);
        var correctPath = imagePath.substr(0, imagePath.lastIndexOf('/') + 1);
        this.copyFileToLocalDir(correctPath, currentName, this.createFileName(currentName));
      }
    }, (err) => {
      this.presentToast('Error while selecting image.');
    });
  }

  private createFileName(currentName) {
    var d = new Date(),
    //n = d.getTime(),
   // newFileName=n+".jpg";
    newFileName;
    //console.log(currentName);
    newFileName = Date.now()+'_'+currentName;
    return newFileName;
  }

  private copyFileToLocalDir(namePath, currentName, newFileName) {
    //console.log("CURRENTFILENAME",currentName);
     this.file.copyFile(namePath, currentName, cordova.file.dataDirectory, newFileName).then(success => {
       this.lastImage = newFileName;
       this.uploadImage();
     }, error => {
        this.presentToast('Error while storing file.');
     });
  }

  public editDetails(data:any){
    this.loading = this.loadingCtrl.create({
      content: 'Uploading...',
    });
    this.loading.present();

    data.last_activity = new Date();
    let filterIntData = _.map(this.InterestDropdownList, function(item) {
        if (item.checked == true) return item;
    });
    //console.log(filterIntData);
    filterIntData = _.without(filterIntData, undefined);

    // let filterUserData = _.map(this.activeUser, function(item1) {
    //     if (item1.checked == true) return item1;
    // });
    // filterUserData = _.without(filterUserData, undefined);

    this.dataService.patchData(data,'UserGroups/'+this.groupId).then((result:any) => {
      let lastGrpId=result.id;
      if(lastGrpId){
        
        // insert group interest
        if(filterIntData.length>0){
          let selectInterestUser = [];
          filterIntData.forEach(element => {
            if(element.id!=''){
              selectInterestUser.push(element.id);
            }
          });  
          if(selectInterestUser.length>0){
            let IntjsonData = {"group_id":lastGrpId,"selectinterest":selectInterestUser};
            this.dataService.postData(IntjsonData,'GroupInterests/insertData').then(res=>{
            
            },err=>{
              
            })
          }
        }

        // insert group user
        // if(filterUserData.length>0){
        //   let selectGrpUser = [];
        //   filterUserData.forEach(element => {
        //     if(element.id!=''){
        //       selectGrpUser.push(element.id);
        //     }
        //   });  
        //   if(selectGrpUser.length>0){
        //     let UsrjsonData = {"group_id":lastGrpId,"selectuser":selectGrpUser};
        //     this.dataService.postData(UsrjsonData,'GroupUsers/insertData').then(res=>{
            
        //     },err=>{
              
        //     })
        //   }
        // }
        
        // insert group activity
        let IntReportData = {"type":1,"description":' has edit the group "'+data.name+'"',"is_delete":0,"customerId":this.userData.id,"notification_pid":this.groupId};
        this.dataService.postData(IntReportData,'reports').then(res=>{
        
        },err=>{
          
        })

        this.loading.dismissAll()
        this.presentToast('Group edited successfully.');       
        this.navCtrl.setRoot('GrouplistPage');
      }else{
        let alert = this.alertCtrl.create({
          title: 'Error!',
          subTitle: 'Something wrong.Please try again.' ,
          buttons: ['Ok']
        });
        alert.present();
      }
    }, (err) => {
      let alert = this.alertCtrl.create({
        title: 'Error!',
        subTitle: this.jsonErrMsg.messageData(err),
        buttons: ['Ok']
      });
      alert.present();
    });
  }
}