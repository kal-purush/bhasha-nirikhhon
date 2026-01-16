import { Component } from '@angular/core';
import { IonicPage, NavController, NavParams, ToastController } from 'ionic-angular';
import { Storage } from '@ionic/storage';
import { ProblemService } from '../../providers/problem-service';

/**
 * Generated class for the ProblemPublishNextPage page.
 *
 * See https://ionicframework.com/docs/components/#navigation for more info on
 * Ionic pages and navigation.
 */

@IonicPage()
@Component({
  selector: 'page-problem-publish-next',
  templateUrl: 'problem-publish-next.html',
})
export class ProblemPublishNextPage {
  user;
  problem;
  label;
  type;
  labelList = [];
  typeList = [];
  selectOptions = {
    title: '请选择问题类型',
    mode: 'md',
    interface: 'action-sheet'
  };
  constructor(
    public navCtrl: NavController,
    public navParams: NavParams,
    public toastCtrl: ToastController,
    public storage: Storage,
    public problemService: ProblemService
  ) {
    this.problem = navParams.get('problem');
  }

  ionViewWillEnter() {
    this.initUser();
    this.problemService.getAllProblemType().subscribe((data) => {
      console.log(data)
      this.typeList = data;
    })
  }

  ionViewCanEnter() {
    if (this.navParams.get('problem') && this.navParams.get('problem').title) {
      return true;
    } else {
      return false;
    }
  }

  initUser() {
    this.storage.get('user').then(user => this.user = user);
  }

  insertLabel() {
    if (this.label.trim() != '') {
      this.labelList.push(this.label);
    }
    this.label = '';
  }

  removeLabel(index) {
    this.labelList.splice(index, 1);
  }

  publish() {
    if (this.labelList.length <= 0) {
      let toast = this.toastCtrl.create({
        message: '至少选择一个标签.',
        duration: 1000,
        showCloseButton: true,
        closeButtonText: '关闭'
      });
      toast.present();
    } else if (!this.type) {
      let toast = this.toastCtrl.create({
        message: '请选择问题类型.',
        duration: 1000,
        showCloseButton: true,
        closeButtonText: '关闭'
      });
      toast.present();
    } else if (!this.user.id) {
      let toast = this.toastCtrl.create({
        message: '用户不存在.',
        duration: 1000,
        showCloseButton: true,
        closeButtonText: '关闭'
      });
      toast.present();

    } else {
      this.problemService.insert(this.type, this.user, this.problem, this.labelList).subscribe(() => {
        let toast = this.toastCtrl.create({
          message: '发送成功',
          duration: 1000,
          showCloseButton: true,
          closeButtonText: '关闭'
        });
        toast.present();
        this.navCtrl.setRoot('TabsPage');
      })
    }
    console.log(this.type, this.user, this.problem, this.labelList);
  }
}