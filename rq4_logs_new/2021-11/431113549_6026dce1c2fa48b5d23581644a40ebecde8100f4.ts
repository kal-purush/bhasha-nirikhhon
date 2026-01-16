import { Component, OnInit } from '@angular/core';
import { Router, ActivatedRoute, Params } from '@angular/router';
import { Observable, map } from 'rxjs';
import { BusStore } from '../bus.store';

@Component({
  selector: 'app-route',
  templateUrl: './route.component.html',
  styleUrls: ['./route.component.scss'],
})
export class RouteComponent implements OnInit {

  private urlParams$: Observable<any> = this.activatedRoute.queryParamMap.pipe(
    map((params: Params) => {
      return {
        city: params['get']('city'),
        route: params['get']('route'),
        direction: params['get']('direction') || 0,
      };
    })
  );

  constructor(
    private router: Router,
    public activatedRoute: ActivatedRoute,
    public busStore: BusStore
  ) {}

  ngOnInit(): void {
    this.urlParams$.subscribe(({ city, route, direction }) => {
      this.busStore.getBusStopOfRoute({ city, route });
      this.busStore.getRealTimeNearStop({ city, route });
      this.busStore.getEstimatedTimeOfArrival({ city, route });
      this.busStore.setCity(city);
      this.busStore.setRoute(route);
    });


    // this.busStore.busStopOfArrival$.subscribe((res) => {
    //   console.log('aaa', res);
    // });
  }

  onDirection(direction: number) {
    this.router.navigate([], {
      queryParams: {
        direction,
      },
      queryParamsHandling: 'merge',
    });
  }
}