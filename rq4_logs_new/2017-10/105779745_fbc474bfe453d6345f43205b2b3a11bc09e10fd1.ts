import { Injectable } from '@angular/core';
import { Http } from '@angular/http';
import 'rxjs/add/operator/map';
import { ActionSheetController } from "ionic-angular";
import { Camera, CameraOptions } from "@ionic-native/camera";
import { FileTransfer, FileUploadOptions, FileUploadResult, FileTransferObject } from '@ionic-native/file-transfer';
import { File } from '@ionic-native/file';
import { ImagePicker } from '@ionic-native/image-picker';

@Injectable()
export class ImgServiceProvider {

  upload: any = {
    url: 'http://xxx/',           //接收图片的url
    fileKey: 'image',  //接收图片时的key
    headers: {
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' //不加入 发生错误！！
    },
    params: {},        //需要额外上传的参数
    success: (data) => { }, //图片上传成功后的回调
    error: (err) => { },   //图片上传失败后的回调
    listen: () => { }   //监听上传过程
  };

  constructor(public http: Http,
    private actionSheetCtrl: ActionSheetController,
    private fileTransfer: FileTransfer,
    private camera: Camera,
    private imagePicker: ImagePicker) {
    console.log('Hello ImgServiceProvider Provider');
  }

  fileTransferObj: FileTransferObject = this.fileTransfer.create();

  // 打开相机
  openCamera() {
    let options: CameraOptions = {
      quality: 50,
      destinationType: this.camera.DestinationType.DATA_URL,
      sourceType: this.camera.PictureSourceType.CAMERA,
      encodingType: this.camera.EncodingType.JPEG,
      mediaType: this.camera.MediaType.PICTURE,
      saveToPhotoAlbum: true
    }
    return this.camera.getPicture(options)
      .then(
        (imageData) => Promise.resolve(imageData),
        (err) => Promise.reject(`Unable to take photo ${err}`)
      );
  }

  // 打开手机相册
  openImgPicker() {
    let imagePickerOpt = {
      maximumImagesCount: 2,
      width: 400,
      height: 300,
      quality: 80
    };
    return this.imagePicker.getPictures(imagePickerOpt)
      .then(
        (results) => Promise.resolve(results),
        (err) => Promise.reject("无法从收集相册中选择图片"));
  }

  // 上传图片
  uploadImg(path: string) {
    if (!path) {
      return;
    }

    let options = {
      fileKey: this.upload.fileKey,
      headers: this.upload.headers,
      params: this.upload.params
    };

    this.fileTransferObj.upload(path, this.upload.url, options)
      .then((data) => {

        if (this.upload.success) {
          this.upload.success(JSON.parse(data.response));
        }

      }, (err) => {
        if (this.upload.error) {
          this.upload.error(err);
        } else {
          // this.noticeSer.showToast('错误：上传失败！');
          alert("图片上传失败！");
        }
      });
  }

  // 停止上传
  stopUpload() {
    if (this.fileTransfer) {
      this.fileTransferObj.abort();
    }
  }

}