import {Component, OnInit} from '@angular/core';
import {Vacationrequest} from '../../models/vacationrequest.model';
import {ActivatedRoute, Router} from '@angular/router';
import {DarService} from '../../dar/dar.service';
import {User} from '../../models/user.model';
import {Drug} from '../../models/drug.model';
import {VacationService} from './vacation.service';

/** @title Datepicker with min & max validation */
@Component({
  selector: 'app-vacation',
  templateUrl: './vacation.component.html',
  styleUrls: ['./vacation.component.css']
})
export class VacationComponent implements OnInit {
  private minDate: Date;
  private vacReq: Vacationrequest;
  private user: User;
  constructor(private router: Router, private route: ActivatedRoute, private service: VacationService) {
    this.vacReq = new Vacationrequest();
  }
  ngOnInit(): void {
    this.minDate = new Date();
    const user = JSON.parse(localStorage.getItem('user'));
    this.user = user;
  }

  confirmVacation() {
    this.service.confirmVacReq(this.user.email, this.vacReq).subscribe( result => {location.reload(); });
  }

  get empty() {
    if (this.vacReq.endDate == null || this.vacReq.startDate == null || this.vacReq.type == null) {
      return true;
    } else {
      return false;
    }
  }
}