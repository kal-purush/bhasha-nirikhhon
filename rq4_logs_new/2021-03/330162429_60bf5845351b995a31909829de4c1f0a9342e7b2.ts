import { HttpClient } from "@angular/common/http";
import { Component, ViewChild, AfterViewInit, OnInit } from "@angular/core";
import { Router, ActivatedRoute, ParamMap } from "@angular/router";
import { MatPaginator, PageEvent } from "@angular/material/paginator";
import { MatSort } from "@angular/material/sort";
import { Location } from "@angular/common";
import {
  merge,
  Observable,
  of as observableOf,
  of,
  pipe,
  range,
  throwError,
  timer,
  zip
} from "rxjs";
import {
  catchError,
  map,
  mergeMap,
  retryWhen,
  startWith,
  switchMap
} from "rxjs/operators";
import pt from "@angular/common/locales/pt";
import { registerLocaleData } from "@angular/common";

@Component({
  selector: "app-empresa",
  templateUrl: "./empresa.html",
  styleUrls: ["./empresa.css"]
})
export class Empresa implements AfterViewInit, OnInit {
  displayedColumns: string[] = ["activity", "amount", "percentage"];
  exampleDatabase: ExampleHttpDatabase | null;
  data: activityItem[] = [];

  resultsLength = 0;
  isLoadingResults = true;
  isRateLimitReached = false;
  errorMessage = "";
  currentPage = 0;

  @ViewChild(MatPaginator) paginator: MatPaginator;
  @ViewChild(MatSort) sort: MatSort;

  constructor(
    private _httpClient: HttpClient,
    private route: ActivatedRoute,
    private location: Location,
    private router: Router
  ) {}

  ngOnInit() {
    registerLocaleData(pt);
  }

  ngAfterViewInit() {
    this.route.queryParams.subscribe(params => {
      this.currentPage = Number(params["pag"]);
    });

    this.paginator.pageIndex = this.currentPage;

    this.exampleDatabase = new ExampleHttpDatabase(this._httpClient);

    // If the user changes the sort order, reset back to the first page.
    this.sort.sortChange.subscribe(() => (this.paginator.pageIndex = 0));

    merge(this.sort.sortChange, this.paginator.page)
      .pipe(
        startWith({}),
        switchMap(() => {
          this.isLoadingResults = true;
          return this.exampleDatabase!.getRepoData(
            this.sort.active,
            this.sort.direction,
            this.paginator.pageIndex
          );
        }),
        map(data => {
          // Flip flag to show that loading has finished.
          this.isLoadingResults = false;
          this.isRateLimitReached = false;
          this.resultsLength = 1000; //data.total_count; // The API does not return max results, limited to 1000 results

          this.location.replaceState(
            "actividades/contratadas?pag=" + this.paginator.pageIndex
          );

          return data.items;
        }),
        backoff(6, 500),
        catchError(error => {
          //alert(JSON.stringify(error));
          //this.errorMessage = error.message;

          this.isLoadingResults = false;
          // Catch if the API has reached its rate limit. Return empty data.
          this.isRateLimitReached = true;
          return observableOf([]);
        })
      )
      .subscribe(data => (this.data = data));
  }
}

export interface ActivitiesRankApi {
  status: string;
  items: activityItem[];
  total_count: number;
}

export interface activityItem {
  code: string;
  levelx: string;
  activity: string;
  amount: number;
  percentage: number;
}

export function backoff(maxTries: number, delay: number) {
  return pipe(
    retryWhen(attempts =>
      zip(range(1, maxTries + 1), attempts).pipe(
        mergeMap(([i, err]) => (i > maxTries ? throwError(err) : of(i))),
        map(i => i * i),
        mergeMap(v => timer(v * delay))
      )
    )
  );
}

/** An example database that the data source uses to retrieve data for the table. */
export class ExampleHttpDatabase {
  constructor(private _httpClient: HttpClient) {}

  getRepoData(
    sort: string,
    order: string,
    page: number
  ): Observable<ActivitiesRankApi> {
    const href =
      "https://f2o3rbv3zd.execute-api.eu-west-1.amazonaws.com/prd/getactivitiesrank";
    const requestUrl = `${href}?page=${page}`;

    return this._httpClient.get<ActivitiesRankApi>(requestUrl);
  }
}