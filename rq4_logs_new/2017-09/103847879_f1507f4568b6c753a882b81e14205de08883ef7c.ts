import { Component, OnInit } from '@angular/core';
import { SearchService } from '../../../services/search/search.service';
import { ActivatedRoute } from '@angular/router';

@Component({
  selector: 'app-full-detail',
  templateUrl: './full-detail.component.html',
  styleUrls: ['./full-detail.component.sass']
})
export class FullDetailComponent implements OnInit {

  result;
  isResultFound = false;
  statistics: any;
  country: string;

  constructor(private route: ActivatedRoute,
              private searchService: SearchService) {
  }

  ngOnInit() {
    this.route.params.subscribe(params => {
      this.country = params['countryName'];
    });

    this.retrieveCountryData();
    // this.retrieveCountryStatistics();
  }

  private retrieveCountryData() {
    this.searchService.searchResults.subscribe(data => {
      console.info('results from component => data', data);
      this.isResultFound = (data.length > 0);
      this.result = data[0];
    });

    this.searchService.getCountryDetails(this.country);
  }

  private retrieveCountryStatistics() {
    this.searchService.getStatisticsByCountry(this.country);
  }

}