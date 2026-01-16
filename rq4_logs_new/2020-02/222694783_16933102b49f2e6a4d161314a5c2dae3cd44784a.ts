import {Component, OnInit} from '@angular/core';
import {ActivatedRoute, Router} from '@angular/router';
import {DarService} from '../../dar/dar.service';
import {User} from '../../models/user.model';
import {Drug} from '../../models/drug.model';
import {SurgeryService} from './surgery.service';
import {Surgery} from '../../models/surgery.model';
import {Room} from '../../models/room.model';

@Component({
  selector: 'app-surgery',
  templateUrl: './surgery.component.html',
  styleUrls: ['./surgery.component.css']
})

export class SurgeryComponent implements OnInit {
  private surgeries: Set<Surgery>;
  private rooms: Set<Room>;
  private doctors: Set<User>;
  private apr: boolean;
  private surgery: Surgery;
  private title: string;

  constructor(private router: Router, private route: ActivatedRoute, private service: SurgeryService) {
    this.surgeries = new Set<Surgery>();
    this.rooms = new Set<Room>();
    this.doctors = new Set<User>();
    this.apr = false;
  }

  ngOnInit(): void {
    this.service.getSurgeries().subscribe(data => {
      this.surgeries = data;
    }, error => {
      alert('Something gone wrong');
    });
  }

  approving(sur: Surgery) {
    this.apr = true;
    this.service.getDoctors(sur).subscribe(data => {this.doctors = data; });
  }

}
