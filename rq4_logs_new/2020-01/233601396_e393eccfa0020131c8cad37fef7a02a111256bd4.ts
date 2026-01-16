import { Injectable } from '@angular/core';
import { Malf } from '../models/malf.model';
import { AngularFirestore } from '@angular/fire/firestore';
import { ToastController } from '@ionic/angular';


@Injectable({
  providedIn: 'root'
})
export class MalfService {


  formData: Malf;


  constructor(private firestore: AngularFirestore, public toastController: ToastController) {

  }
  getEmployees() {
    return this.firestore.collection('malfs').snapshotChanges();
  }

  save(value) {
    let data = Object.assign({}, value);
    delete data.id;
    if (value.id == null) {
      this.firestore.collection('malfs').add(data);
    } else {
      this.firestore.doc('malfs/' + value.id).update(data)
    }
    this.presentToast('Your data have been saved!')
  }

  async presentToast(message: string) {
    const toast = await this.toastController.create({
      message,
      duration: 2000
    });
    toast.present();
  }

  lastMalf(){
    let dateNow=`${this.dateStamp()}`
    console.log(dateNow)
    
    return this.firestore.collection('malfs', ref => ref.where("date", '>=', dateNow).orderBy("date","asc").limit(3))
  }
  lastThreeMalf(){
    let dateNow=`${this.dateStamp()}`
    console.log(dateNow)
    
    return this.firestore.collection('malfs', ref => ref.where("date", '<', dateNow).orderBy("date","desc").limit(3))
  }

  dateStamp(){
    let todayDay=new Date().setHours(0,0,0,0)
        return todayDay
  }



}