import { Component } from '@angular/core';
import { IonicPage, NavController, NavParams } from 'ionic-angular';
import { Storage } from '@ionic/storage';
import { AccountServiceProvider } from '../../providers/account.service/account.service';
import { ProfileServiceProvider } from '../../providers/profile.service/profile.service';
import { Element } from '@angular/compiler';
@IonicPage()
@Component({
  selector: 'page-picture-upload',
  templateUrl: 'picture-upload.html'
})
export class PictureUploadPage {
  image: any;
  uploadPhotoDisplay: any;
  userId: string;
  account;
  profile;
  constructor(
    public navCtrl: NavController,
    public navParams: NavParams,
    public storage: Storage,
    public accountServiceProvider: AccountServiceProvider,
    public profileServiceProvider: ProfileServiceProvider
  ) {}

  async ionViewDidLoad() {
    console.log('ionViewDidLoad PictureUploadPage');
    this.storage.get('userId').then(userId => {
      (async () => {
        this.userId = userId;
        //this.account = await this.accountServiceProvider.getAccount(
        //  this.userId
        // );
        this.profile = await this.profileServiceProvider.getProfileByEmail(
          'alex.hoffman@excella.com' //this.account.email
        );
        var x = await this.profileServiceProvider.getProfileByEmail(
          'alex.hoffman@excella.com'
        );
      })();
    });
  }

  uploadPicture() {
    var that = this;
    const cloudName = 'excella';
    const unsignedUploadPreset = 'kwoafltg';

    var url = `https://api.cloudinary.com/v1_1/${cloudName}/upload`;
    var xhr = new XMLHttpRequest();
    var fd = new FormData();
    xhr.open('POST', url, true);
    xhr.setRequestHeader('X-Requested-With', 'XMLHttpRequest');

    xhr.onreadystatechange = function(e) {
      if (xhr.readyState == 4 && xhr.status == 200) {
        // File uploaded successfully
        var response = JSON.parse(xhr.responseText);
        // https://res.cloudinary.com/cloudName/image/upload/v1483481128/public_id.jpg
        var url = response.secure_url;
        // Create a thumbnail of the uploaded image, with 150px width
        var tokens = url.split('/');
        tokens.splice(-2, 0, 'w_150,c_scale');
        var img = new Image(); // HTML5 Constructor
        img.src = tokens.join('/');
        img.alt = response.public_id;
      }
    };

    fd.append('upload_preset', unsignedUploadPreset);
    fd.append('tags', 'browser_upload'); // Optional - add tag for image admin in Cloudinary
    fd.append('file', this.image, 'test.png');
    xhr.onload = function() {
      that.profile.avatarUrl = JSON.parse(xhr.response).url;
      that.profileServiceProvider.updateProfileById(that.profile);
    };
    xhr.send(fd);
  }

  imgChange(event) {
    var that = this;
    this.image = event.srcElement.files[0];
    var photoToUpload = document.getElementById(
      'photo-to-upload'
    ) as HTMLImageElement;
    var reader = new FileReader();
    reader.onloadend = function() {
      photoToUpload.src = reader.result;
    };
    reader.readAsDataURL(this.image);
  }
}