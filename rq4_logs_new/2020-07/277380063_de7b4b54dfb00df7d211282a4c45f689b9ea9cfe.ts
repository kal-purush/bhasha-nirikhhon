import { Component, OnInit } from '@angular/core';
import { AngularFireDatabase, AngularFireList } from 'angularfire2/database';
import { Observable } from 'rxjs/Observable';
import 'rxjs/add/operator/map';

@Component({
  selector: 'my-app',
  templateUrl: './app.component.html',
  styleUrls: [ './app.component.css' ]
})
export class AppComponent implements OnInit {
  name = 'Angular 5';
  commentList:AngularFireList<any>;
  postList:AngularFireList<any>;
  posts:any[];
  comments:any[];
  itemsRef: AngularFireList<any>;
  items: Observable<any[]>;
  constructor(public afd: AngularFireDatabase) {
    this.itemsRef = afd.list('posts');
    // Use snapshotChanges().map() to store the key
    this.items = this.itemsRef.snapshotChanges().map(changes => {
      return changes.map(c => ({ key: c.payload.key, ...c.payload.val() })      );
    });
  }


  ngOnInit() { }

  addItem(newName: string) {
    this.itemsRef.push({
      title:'This is first post',
      Description: 'Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industrys standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum.',
      createdBy:'Lokesh Daiya',
      createdByEmpId:'e5313410',
      createdByEmailId:'lokesh.daiya@gmail.com',
      businessImpact:'High',
      status:'open',
      technology:'finance, angular',
      challengeAria:'performance',
      
    });
  }

  getDataByKey(){
    const data = this.afd.object('/posts/-LBzGxbpqtDDfOYZfpY0').valueChanges().subscribe(val => {
      console.log(val);
    });
    
  }
  updateItem(key: string) {
    const key1 ='-LBzGxbpqtDDfOYZfpY0'
    const data = this.afd.object(`/posts/${key1}`).valueChanges().subscribe((val:any) => {
      const appliedBy = val.appliedBy || [];

      appliedBy.push({
        name:'Sandeep Patidar',
        comment:'I can do this in 5 days',
      })
      //console.log(val)
      this.itemsRef.update(key1, { appliedBy: appliedBy });
    });
    //const appliedBy = this.items.appliedBy || [];
    //this.itemsRef.update(key, { appliedBy: 'test' });
  }
  deleteItem(key: string) {    
    this.itemsRef.remove(key); 
  }
  deleteEverything() {
    this.itemsRef.remove();
  }


  getComments() {
   this.commentList = this.afd.list('/comments');
   return this.commentList;
  }

  getPost() {
    this.postList = this.afd.list('/posts')
    return this.postList;
  }

  insertPost(){
    this.postList.push({
      title:'This is first post',
      Description: 'Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industrys standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum.',
      createdBy:'Lokesh Daiya',
      createdByEmpId:'e5313410',
      createdByEmailId:'lokesh.daiya@gmail.com',
      businessImpact:'High',
      status:'open',
      technology:'finance, angular',
      challengeAria:'performance',
      
    })
  }

  insertComments() {
    this.commentList.push({
      title : 'Test Comment1',
      description : 'Test Descirption1',
      addedAt : Date.now(),
      updateAt : Date.now()
    })
  }
}


export class CommentModel {
  $key : string;
  title : string;
  description : string;
  addedAt : Date;
  updateAt : Date;
}