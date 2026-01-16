import {inject} from 'aurelia-framework';
import {HttpClient} from 'aurelia-fetch-client';
import {IQueryLanguageOptions, QueryLanguage} from 'marvelous-aurelia-query-language';
import config from 'config';

@inject(HttpClient)
export class Basic {
  tableRows: any[];
  http: HttpClient;
  queryLanguage: QueryLanguage;
  queryLanguageOptions: IQueryLanguageOptions;
  
  samples = ['FirstName contains abel and [Age > 40 or Age < 30]', 'Rating equals NONE'];
  
  constructor(http: HttpClient) {
    this.queryLanguageOptions = {
      autoComplete: config.apiUrl('api/mql/syntax/people/auto-completion'),
      inlineButton: true,
      inlineButtonText: 'Apply',
      onSubmit: () => this.submit()
    };

    http.configure(httpConfig => {
      httpConfig
        .useStandardConfiguration()
        .withBaseUrl(config.apiUrl('api/'));
    });

    this.http = http;
  }
  
  activate() {
    return this.http.fetch('mql/syntax/people')
      .then(response => response.json())
      .then(x => this.tableRows = x.data);
  }
  
  submit() {
    return this.http.fetch('mql/syntax/people?query=' + this.queryLanguage.query)
      .then(response => response.json())
      .then(x => { this.tableRows = x.data; return x; });
  }
  
  setQuery(query: string) {
    this.queryLanguage.query = query;
    this.submit();
  }
}