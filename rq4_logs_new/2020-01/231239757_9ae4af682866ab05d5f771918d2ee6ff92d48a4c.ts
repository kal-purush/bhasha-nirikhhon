import { Component, OnInit } from '@angular/core';
import { DataAPIService } from '../../services/dataAPI.service';
import { componentDestroyed } from '../../core/takeUntil-function/takeUntil-function';
import { takeUntil } from 'rxjs/operators';
import { GraphService } from '../../graph/graph-service/graph.service';

@Component({
  selector: 'app-dashboard',
  templateUrl: './dashboard.component.html',
  styleUrls: ['./dashboard.component.css']
})
export class DashboardComponent implements OnInit {
  P1 = [{ element: 1, round: 1, total_points: 20 }, { element: 1, round: 111, total_points: 12 }, { element: 1, round: 1, total_points: 4 }, { element: 1, round: 111, total_points: 12 }, { element: 1, round: 1, total_points: 20 }, { element: 1, round: 111, total_points: 12 }];
  P2 = [{ element: 2, round: 13, total_points: 30 }, { element: 2, round: 222, total_points: 4 }, { element: 1, round: 1, total_points: 20 }, { element: 1, round: 111, total_points: 12 }, { element: 1, round: 1, total_points: 20 }, { element: 1, round: 111, total_points: 12 }];
  P3 = [{ element: 3, round: 1, total_points: 40 }, { element: 3, round: 333, total_points: 23 }, { element: 1, round: 1, total_points: 20 }, { element: 1, round: 111, total_points: 12 }, { element: 1, round: 1, total_points: 20 }, { element: 1, round: 111, total_points: 12 }];
  A = [this.P1, this.P2, this.P3];
  PLAYERSLIST = [];
  DATA;
  testDATA = [];

  constructor(
    private dataService: DataAPIService,
    private graphService: GraphService
  ) { }

  ngOnInit() {
    this.getPlayers();

    //this.DATA = this.A;

    setTimeout(() => {
    //  this.DATA = this.A;
    }, 2)
    // this.getPlayerHistory(1);
  }
  ngOnDestroy(): void {
  }

  getPlayers() {
    this.dataService.getAllPlayers()
      .pipe(takeUntil(componentDestroyed(this)))
      .subscribe(
        data => {
          this.PLAYERSLIST = data;
          console.log(data);
        }
      )
  }
  onPlayerSelect(e) {
    console.log(e.first_name);
    //this.getPlayerHistory(e.id);
    this.graphService.setPlayersLIST(e);
  }
  getPlayerHistory(id) {
    // this.dataService.getPlayerHistory(id)
    //   .pipe(takeUntil(componentDestroyed(this)))
    //   .subscribe(
    //     data => this.setPlayersListInGraph(data)
    //   )
  }

  setPlayersListInGraph(data) {
    this.filterTotalPoints(data);

    // if(this.DATA==undefined||this.DATA==[]){
    //   this.DATA=data;
    // }
    // else{

    // }
    if (!this.DATA) {
      this.DATA = [];
    }

    this.DATA.push(data);
  //  console.log(this.DATA);
    this.graphService.setPlayersLIST(data);
    // let tempDATA = this.DATA;
    // this.DATA = [];
    // setTimeout(()=>{
    //   console.log(this.DATA);
    //   this.DATA = tempDATA;
    // },200);

  }
  filterTotalPoints(data) {

  }
}