import {Component, OnInit} from '@angular/core';
import {AuthService} from '../auth/auth.service';
import {CompanieService} from './companie.service';
import {RegionService} from '../region/region.service';
//import {RegionComponent} from '../region/region.component';
import {Companie} from './companie.model';
import {ChangeDetectionStrategy, Input} from "@angular/core";
import {NgbModal, NgbActiveModal} from '@ng-bootstrap/ng-bootstrap';
import {ToastsManager} from 'ng2-toastr';
import { Inject, forwardRef} from '@angular/core';
import {MdDialog, MdDialogRef} from '@angular/material';
import { Router, ActivatedRoute, Params } from '@angular/router';

@Component({
  selector: 'app-companie',
  templateUrl: './companieDetail.component.html',
  styleUrls: ['./companie.component.css'],
})
export class CompanieDetailComponent implements OnInit {
  fetchedCompanie : {}



  constructor(
    private companieService: CompanieService,
    private regionService: RegionService,
    private modalService: NgbModal,
    private toastr: ToastsManager,
    public dialog: MdDialog,
    private route: ActivatedRoute,
    private router: Router,
  ) {
    this.getCompanie('58dd78fc734d1d01a238aa4b');
  }

  ngOnInit() {
  }

  onDelete(id: string) {
    this.companieService.deleteCompanie(id)
      .subscribe(
        res => {
          this.toastr.success('Great!', res.message);
          console.log(res);
        },
        error => {
          console.log(error);
        }
      );
  }



  getCompanie(id) {

    this.route.params
      .switchMap((params: Params) => this.companieService.getCompanie(params['id']))
  //    .subscribe((hero: Hero) => this.hero = hero);

    //this.companieService.getCompanie(id)
      .subscribe(
        res => {
        //  console.log("companies");
        //  console.log(res);
          this.fetchedCompanie = res
        },
        error => {
          console.log(error);
        }
      );
  }

}