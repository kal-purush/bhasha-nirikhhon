import { Injectable } from '@angular/core';
import { ApolloQueryResult } from '@apollo/client/core';

@Injectable({
  providedIn: 'root',
})
export class QueryResponseService {
  handleRes<T>(
    response: ApolloQueryResult<T>,
    successHandler: (res: ApolloQueryResult<T>) => void
  ) {
    if (response.errors || response.error) {
      this.handleError(response.errors[0].message);
    } else {
      successHandler(response);
    }
  }

  handleError(error: string) {
    console.error('something bad happened', error);
  }
}