import { File } from '@ionic-native/file/ngx';
import { Camera, CameraOptions } from '@ionic-native/camera/ngx';
import { Crop } from '@ionic-native/crop/ngx';
import { AlertController } from '@ionic/angular';
import * as AWS from 'aws-sdk';

export class EducfinamentUploadAvatar {
  private file: File = new File();
  private camera: Camera = new Camera();
  private crop: Crop = new Crop();
  private cloudCredentials: any = {};
  private isUploadingCallback: any;
  private alertController: AlertController;
  private avatarUrl: string = "https://educfinament.s3-us-west-2.amazonaws.com/avatars/default.jpg";

  constructor(_cloudCredentials: any) {
    this.cloudCredentials = _cloudCredentials;
    this.alertController = new AlertController();
  }

  public getImageFromLibrary(_isUploadingCallback: any) {
    this.isUploadingCallback = _isUploadingCallback;
    return this.getImageAndUploadToS3(this.camera.PictureSourceType.PHOTOLIBRARY);
  }

  private getImageAndUploadToS3(sourceType: any) {

    return new Promise<string>(function(resolve, reject) {

      let options: CameraOptions = {
        quality: 100,
        sourceType: sourceType,
        destinationType: this.camera.DestinationType.FILE_URI,
        encodingType: this.camera.EncodingType.JPEG,
        mediaType: this.camera.MediaType.PICTURE
      };

      let uuid: string = Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
      let target_filename: string = (new Date().getTime()).toString() + "-" + uuid + "-avatar.jpg";

      this.camera.getPicture(options).then(imageData => {
        this.crop.crop(imageData, { quality: 50, targetWidth: 128, targetHeight: 128 })
          .then((newPath: string) => {

            var imagePath: string = newPath.split('?')[0];
            var copyPath = imagePath;
            var splitPath = copyPath.split('/');
            var imageName = splitPath[splitPath.length - 1];
            var filePath = imagePath.split(imageName)[0];

            this.isUploadingCallback();
            this.uploadFile(filePath+imageName, target_filename).then((avatarKey: any) => {
              resolve("https://educfinament.s3-us-west-2.amazonaws.com/" + avatarKey);
            }, (error: any) => {
              this.showAlert("ERROR: " + error);
              reject(error);
            });

          },
            (error: any) => {
              this.showAlert("ERROR: " + error);
              reject(error);
            }).catch((error: any) => {
              this.showAlert("ERROR: " + error);
              reject(error);
            });

      }, (error: any) => {
        this.showAlert("Has de seleccionar una imatge.");
        reject(error);
      });
    }.bind(this));

  }

  private uploadFile(filename: string, target_filename: string) {
    return new Promise(function(resolve, reject) {
      this.file.resolveLocalFilesystemUrl(filename).then(fileEntry => {
        let path: string = filename.replace(fileEntry.name, "");

        this.file.readAsArrayBuffer(path, fileEntry.name)
          .then((videoData: ArrayBuffer) => {
            AWS.config.update(this.cloudCredentials);
            var config = {
              region: "us-west-2",
              useDualstack: false,
              useAccelerateEndpoint: false,
              s3ForcePathStyle: false,
              maxRetries: 0
            };

            var s3 = new AWS.S3(config);
            let key: string = "avatars/" + target_filename;
            s3.putObject({ Bucket: 'educfinament', Key: key, Body: videoData }, function(err, data) {
              if (err) {
                reject(err);
              } else {
                resolve(key);
              }
            }.bind(this));
          })
          .catch((error) => {
            reject(error);
          });
      }).catch((error) => {
        reject(error);
      });
    }.bind(this));
  }

  async showAlert(msg: string) {
    const alert = await this.alertController.create({
      header: 'Alert',
      subHeader: '',
      message: msg,
      buttons: ['OK']
    });

    await alert.present();
  }

}