import { Component, OnInit } from "@angular/core";
import { Router, ActivatedRoute } from "@angular/router";
import { FormControl, FormGroup, Validators } from "@angular/forms";
import { ApiService } from "../../services/apiservice";
@Component({
  selector: "app-viewpoll",
  templateUrl: "./viewpoll.component.html",
  styleUrls: ["./viewpoll.component.css"]
})
export class ViewpollComponent implements OnInit {
  pollview: any;
  id: any;
  titleEdit: any;
  loading: boolean;
  errorMessage: String;
  constructor(
    private apiServices: ApiService,
    private router: Router,
    private activatedRoute: ActivatedRoute
  ) {}

  ngOnInit() {
    this.addPollForm();
  }
  addPollForm() {
    this.titleEdit = new FormGroup({
      title: new FormControl("", [Validators.required, Validators.minLength(4)])
    });
  }
  onSubmit(formData) {
    this.loading = true;
    this.id = this.activatedRoute.snapshot.paramMap.get("id");
    this.apiServices.editpolltitle(this.id, formData.value).subscribe(res => {
      this.loading = false;
      this.router.navigate(["/list"]);
      this.titleEdit.reset();
    });
  }
}