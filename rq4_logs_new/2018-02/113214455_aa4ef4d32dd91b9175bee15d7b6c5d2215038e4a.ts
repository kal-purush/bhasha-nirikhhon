
//REQUIRED DEPENDENCIES AND LIBRARIES
import { FirebaseDatabase } from './FirebaseDatabase';
import { SQLiteHandler } from './SQLiteHandler';


//CLASS
export class StorageHandler {
  
  //VARIABLE
  static storageObject: StorageHandler;
  sqliteObject = SQLiteHandler.getInstance();
  firebaseDatabaseObject = FirebaseDatabase.getInstance();


  /** 
   * Method Name   : getInstance()
   * Purpose       : to get the instance of StorageHandler class
   * Trigger when  : invoked by  
   **/
  public static getInstance() {
    if(!this.storageObject){
      this.storageObject = new StorageHandler();
    }
    return this.storageObject;
  }


  public checkDataExistence() {

  }


  public updateDataSets() {

  }
}