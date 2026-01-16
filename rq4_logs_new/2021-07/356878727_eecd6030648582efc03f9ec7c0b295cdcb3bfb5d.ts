import {Apollo} from "apollo-angular";
import {catchError, throwError} from "rxjs";
import {map} from "rxjs/operators";
import {ApolloError} from "@apollo/client";
import {extractApolloResponse, IUser, TApolloResponse} from "@modern-app/shared/utils/interfaces";

import * as UserQueries from '../graphql/user.queries';
import {IUserApollo} from "../interfaces/user-apollo.interface";

export class UserApollo implements IUserApollo {
  constructor(private apollo: Apollo) {
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  loadUser(queryParams: Record<string, unknown>): TApolloResponse<IUser> {
    return this.apollo.query<{ user: IUser }>({query: UserQueries.userRequest.query}).pipe(
      map(result => extractApolloResponse(result, UserQueries.userRequest.keys)),
      catchError((error: ApolloError) => throwError(() => error)),
    );
  }

}