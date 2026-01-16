import { Component, OnInit } from '@angular/core';
import { CommonService } from 'src/app/services/common.service';
import { PostService } from 'src/app/services/post.service';

@Component({
  selector: 'app-default',
  templateUrl: './default.component.html',
  styleUrls: ['./default.component.scss']
})
export class DefaultComponent implements OnInit {
  
  isSidebar: boolean = false;

  constructor(
    private postService: PostService,
    private commonServices: CommonService) {
    this.commonServices.sidebar_collapse.subscribe((res: any) => this.isSidebar = res);
  }



  ngOnInit(): void {
    this.loadDashboard();
  }

  response: any;
  loadDashboard() {
    var postData = {};
    // this.postService.dashboard(postData)
    //   .subscribe({
    //     next: (response: any) => {
    //       console.log('response : ' + response);
    //       this.response = response;
    //     },
    //     error: (err) => console.log(err)
    //   })
  }
}