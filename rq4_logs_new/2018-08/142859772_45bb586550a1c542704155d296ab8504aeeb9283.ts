import { Component, OnInit } from '@angular/core';
import { Router, ActivatedRoute } from "@angular/router";
import { ApiService } from "../../services/apiservice";
import { user } from "../model";
@Component({
  selector: "app-listusers",
  templateUrl: "./listusers.component.html",
  styleUrls: ["./listusers.component.css"]
})
export class ListusersComponent implements OnInit {
  user: Array<user>;
  loader: boolean;
  errorMessage: string;
  constructor(
    private apiServices: ApiService,
    private router: Router,
    private activatedRoute: ActivatedRoute
  ) {}

  ngOnInit() {
    this.listusers();
  }
  listusers() {
    this.loader = true;
    this.apiServices.listusers().subscribe(res => {
      if (res["error"]) {
        this.errorMessage = res["message"];
      }
      else  {
        this.loader = false;
        this.user = res["data"].reverse();
      }
    });
  }
 
}