/**
 * Created by Sivan on 6/5/2017.
 */
import {Camera, CameraOptions } from '@ionic-native/camera';
import { Injectable } from '@angular/core';
import {LoadingController, ActionSheetController, Platform, AlertController} from 'ionic-angular';
import {User} from "./user";
import * as firebase from 'firebase';
import {File} from "@ionic-native/file";

@Injectable()
export class Avatar {
  currentUser: any;
  private recordPath: string;
  private userID: string;
  private loading: any;

  constructor(public actionSheeCtrl: ActionSheetController,
              public loadingCtrl: LoadingController,
              private camera: Camera,
              private _user: User,
              private platform: Platform,
              private file1: File,
              private alertCtrl: AlertController) {
    this._user.currentUser.subscribe((data) => {
      this.currentUser = data;
    });
  }

  openImageOptions() {
    var self = this;

    let actionSheet = self.actionSheeCtrl.create({
      title: 'Upload new image from',
      buttons: [
        {
          text: 'Camera',
          icon: 'camera',
          handler: () => {
            self.openCamera(self.camera.PictureSourceType.CAMERA);
          }
        },
        {
          text: 'Album',
          icon: 'folder-open',
          handler: () => {
            self.openCamera(self.camera.PictureSourceType.PHOTOLIBRARY);
          }
        }
      ]
    });

    actionSheet.present();
  }


  openCamera(pictureSourceType: any) {
    var self = this;

    let options: CameraOptions = {
      quality: 95,
      destinationType: self.camera.DestinationType.DATA_URL,
      sourceType: pictureSourceType,
      encodingType: self.camera.EncodingType.JPEG,
      targetWidth: 128,
      targetHeight: 128,
      allowEdit: true,
      saveToPhotoAlbum: true,
      correctOrientation: true
    };

    self.camera.getPicture(options).then(imageData => {
      const b64toBlob = (b64Data, contentType = '', sliceSize = 512) => {
        const byteCharacters = atob(b64Data);
        const byteArrays = [];

        for (let offset = 0; offset < byteCharacters.length; offset += sliceSize) {
          const slice = byteCharacters.slice(offset, offset + sliceSize);

          const byteNumbers = new Array(slice.length);
          for (let i = 0; i < slice.length; i++) {
            byteNumbers[i] = slice.charCodeAt(i);
          }

          const byteArray = new Uint8Array(byteNumbers);

          byteArrays.push(byteArray);
        }

        const blob = new Blob(byteArrays, {type: contentType});
        return blob;
      };

      let capturedImage: Blob = b64toBlob(imageData, 'image/jpeg');

      self.startUploading(capturedImage);
    }, error => {
      console.log('ERROR -> ' + JSON.stringify(error));
    });
  }

  startUploading(file) {

    let self = this;
    let uid = self.currentUser.userID;
    let progress: number = 0;
    // display loader
      let loader = this.loadingCtrl.create({
      content: 'Uploading image..',
    });

    loader.present();
     var
     metadata = {
     contentType: 'image/jpeg',
     name: 'profile.jpeg',
     cacheControl: 'no-cache',
     };

     var uploadTask = firebase.storage().ref().child('client-data/images/' + uid + '/profile.jpeg').put(file, metadata);
    // Listen for state changes, errors, and completion of the upload.
    uploadTask.on('state_changed',
      function (snapshot) {
        // Get task progress, including the number of bytes uploaded and the total number of bytes to be uploaded
        progress = (snapshot.bytesTransferred / snapshot.totalBytes) * 100;
      }, function (error) {
        loader.dismiss().then(() => {
          switch (error.name) {
            case 'storage/unauthorized':
              // User doesn't have permission to access the object
              break;

            case 'storage/canceled':
              // User canceled the upload
              break;

            case 'storage/unknown':
              // Unknown error occurred, inspect error.serverResponse
              break;
          }
        });
      }, function () {
     loader.dismiss().then(() => {
     // Upload completed successfully, now we can get the download URL
    var downloadURL = uploadTask.snapshot.downloadURL;
       self.currentUser.image = downloadURL;
       self._user.updateUser(self.currentUser);

     });
      });
  }


}