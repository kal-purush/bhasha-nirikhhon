import { Component, OnInit } from '@angular/core';
import { Apollo } from 'angular2-apollo';

import gql from 'graphql-tag';

import client from '../client';

@Component({
  selector: 'app',
  template: `
    Hello World
  `
})
@Apollo({
  client,
  queries: (component: AppComponent) => ({
    data: {
      query: `
        query getThings {
          things {
            id
          }
        }
      `
    }
  })
})
export class AppComponent {
  data: any;
}