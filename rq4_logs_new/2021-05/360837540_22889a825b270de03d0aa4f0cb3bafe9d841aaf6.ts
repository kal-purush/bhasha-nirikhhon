import { Component, OnInit } from '@angular/core';
import { Subscription } from 'rxjs';

@Component({
  selector: 'app-proposals',
  templateUrl: './proposals.component.html',
  styleUrls: ['./proposals.component.css']
})
export class ProposalsComponent implements OnInit {

  // public posts: Post[] = [];
  // private records: Records[] = [];
  private subscription: Subscription = new Subscription();

  constructor() { }

  ngOnInit(): void {
    // this.subscription
    //   .add(this.getProposals())
  }

  // private getProposals(): Subscription {
    // tslint:disable-next-line: deprecation
    // return this.postsService.getPosts().subscribe((record: Records) => {
    //   this.records.push(record);
    //   this.posts = this.records[0].records;
    //   this.paginationPosts = this.posts.slice(0, this.pageSize);
    //   this.length = this.posts.length;
    //   this.loading = true;
    // });
  // }

}