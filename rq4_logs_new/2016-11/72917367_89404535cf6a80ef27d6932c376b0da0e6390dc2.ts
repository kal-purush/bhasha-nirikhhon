import { Component } from '@angular/core';
import { NavController } from 'ionic-angular';
import { Http } from '@angular/http';

@Component({
  selector: 'page-diskusi',
  templateUrl: 'diskusi.html'
})
export class DiskusiPage {
  private status;
  private nama;
  private test;
  private diskusi;
  private isi;

  private response;

  constructor(public navCtrl: NavController, public http: Http) {

  }

  ionViewDidLoad() {
    let ivan;
    ivan = ''
    console.log('Hello DiskusiPage Page');

    this.http.get('http://cybex.agri.web.id/api/all_diskusi.php').subscribe(res => {
      // this.status = res.json();
      this.diskusi = res.json();
      // console.log(this.nama);
    });
  }

  kirim() {
    let send = JSON.stringify({value : this.isi});

    this.http.post('http://cybex.agri.web.id/api/test.php', send).subscribe(res => {
      this.response = res;

      if (res) {
        this.isi = '';
        this.response = '';
      }
    })
  }

}