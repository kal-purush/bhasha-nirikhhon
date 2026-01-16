import * as Relay from "react-relay";

export class UserRoute extends Relay.Route {
  public static queries = {
    user: () => Relay.QL`
      query {
        user
      }
    `,
  };
  public static routeName = "UserRoute";
}