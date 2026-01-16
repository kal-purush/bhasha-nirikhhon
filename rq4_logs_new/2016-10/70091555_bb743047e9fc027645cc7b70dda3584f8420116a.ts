import { Component, OnInit, Input } from '@angular/core';
import { SessionService, Company } from '../session/session.service';
import { BenchmarksService } from './benchmarks.service';
import { Router } from '@angular/router';

@Component({
  selector: 'app-benchmarks',
  templateUrl: './benchmarks.component.html',
  styleUrls: ['./benchmarks.component.css'],
  providers: [BenchmarksService] // declared here as it only services this component
})
export class BenchmarksComponent implements OnInit {
  @Input() company: Company;

    constructor(private sessionService: SessionService, private router: Router, private benchmarksService: BenchmarksService) {

    }

    ngOnInit() {

    }

}