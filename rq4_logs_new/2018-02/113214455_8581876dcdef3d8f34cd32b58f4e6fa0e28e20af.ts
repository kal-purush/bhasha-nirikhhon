
//REQUIRED DEPENDENCIES AND LIBRARIES
import { File } from '@ionic-native/file';
import { Dates } from './Dates';
declare var cordova: any;

//CLASS
export class DirectoryHandler {

  //VARIABLES
  static directoryObject: DirectoryHandler;
  datesObject = Dates.getInstance();
  path;
  

  /** 
   * Method Name   : getInstance()
   * Purpose       : to get the instance of Path class
   * Trigger when  : invoked by DatabaseHandler, ProcessPage
   **/
  public static getInstance() {
    if(!this.directoryObject){
      this.directoryObject = new DirectoryHandler();
    }
    return this.directoryObject;
  }


  /** 
   * Method Name   : getFile()
   * Purpose       : to get the instance of File class
   * Trigger when  : invoked by checkDirectory(), createDirectory()
   **/
  public getFile() {
    return new File();
  }


  /** 
   * Method Name   : checkDirectory()
   * Purpose       : to check whether directory "Water Turbidity Meter" already exists in the 
   *                 application directory or not. Return false if the directory does not exist.
   * Trigger when  : invoked by createDirectory()
  **/
  public checkDirectory(){
    this.getFile().checkDir(cordova.file.externalRootDirectory, 'Water Turbidity Meter').then(_ => {
      return true;
    }, (err) => {
      return false;
    });
  }
  

  /** 
   * Method Name   : createDirectory()
   * Purpose       : to create directory "Water Turbidity Meter & Images" if checkDirectory() 
   *                 returns false which means the directory need to be created since it does not exist.
   * Trigger when  : invoked by constructor()
  **/
  public createDirectory() {
    if(!this.checkDirectory()) {
      //create directory Water Turbidity Meter
      this.getFile().createDir(cordova.file.externalRootDirectory, 'Water Turbidity Meter', true).then(_ => {
        //create directory Images
        var path = cordova.file.externalRootDirectory + 'Water Turbidity Meter/';
        this.getFile().createDir(path, 'Images', true).then(_ => {}).catch(err => {});
      }).catch(err => {var error = JSON.stringify(err);});
    }
  }


  /** 
   * Method Name   : getPath()
   * Purpose       : to get the path where all images are stored in Application
   * Trigger when  : invoked by pathForImage()
  **/
  public getPath() {
    return cordova.file.externalRootDirectory + 'Water Turbidity Meter/Images/';
  }


  /** 
   * Method Name   : getFileName()
   * Purpose       : to get the name of image stored in Application
   * Trigger when  : invoked by pathForImage()
  **/
  public getFileName() {
    return this.datesObject.date + '.jpg';
  }


  /** 
   * Method Name   : pathForImage()
   * Purpose       : to show image in Application
   * Trigger when  : invoked by ProcessPage copyFileToLocalDir(), DatabaseHandler insertData()
  **/
  public pathForImage() {
    this.path = this.getPath() + this.getFileName();
    return this.path;
  }


  /** 
   * Method Name   : convertToDataURL()
   * Purpose       : to convert image to base64 formatted string
   * Trigger when  : invoked by FirebaseStorage 
  **/
  public convertToDataURL(fileName){
    var image;
    this.getFile().readAsDataURL(this.getPath(), fileName).then((data) => {
      image = data;
    }).catch((err) => console.log(err));
    return image;
  }
}