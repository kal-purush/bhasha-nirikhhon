import { Component, Input, OnInit } from "@angular/core";

@Component({
  selector: "app-list-posts",
  templateUrl: "./list-posts.component.html",
  styleUrls: ["./list-posts.component.scss"]
})
export class ListPostsComponent implements OnInit {
  @Input() title: string;
  @Input() content: string;

  constructor() {}

  ngOnInit() {}
}