import {Http, BaseRequestOptions, Response, ResponseOptions, RequestMethod} from '@angular/http';
import {MockBackend, MockConnection} from '@angular/http/testing';
import {User} from './../user';
import {forEach} from "@angular/router/src/utils/collection";
import {zipStatic} from "rxjs/operator/zip";

export let fakeBackendProvider = {
  // use fake backend in place of Http service for backend-less development
  provide: Http,
  useFactory: (backend: MockBackend, options: BaseRequestOptions) => {
    // configure fake backend
    backend.connections.subscribe((connection: MockConnection) => {
      let testUsers = [
        new User(1, 'Stephan', 'Keel', User.ADMIN, 'keel', 'gugus'),
        new User(2, 'David', 'Leuenberger', User.ADMIN, 'leuenberger', 'david'),
        new User(3, 'Hans', 'Müller', User.STANDARD, 'mueller', '1234'),
        new User(4, 'Gast', 'Gast', User.GUEST, 'gast', 'gast'),
      ];

      // wrap in timeout to simulate server api call
      setTimeout(() => {

        // fake authenticate api end point
        if (connection.request.url.endsWith('/api/authenticate') && connection.request.method === RequestMethod.Post) {
          // get parameters from post request testUser
          let params = JSON.parse(connection.request.getBody());

          // check user credentials and return fake jwt token if valid
          let matchedUsers = testUsers.filter(u => params.username === u.username && params.password === u.password);
          if (matchedUsers.length > 0) {
            connection.mockRespond(new Response(
              new ResponseOptions({status: 200, body: {token: 'fake-jwt-token ' + matchedUsers[0].id}})
            ));
          } else {
            connection.mockRespond(new Response(
              new ResponseOptions({status: 200})
            ));
          }
        }

        // fake users api end point
        if (connection.request.url.startsWith('/api/users')) {
          let authHeader = connection.request.headers.get('Authorization');
          if (!authHeader || !authHeader.startsWith('Bearer fake-jwt-token ')) {
            // return 401 not authorised if token is null or invalid
            connection.mockRespond(new Response(
              new ResponseOptions({status: 401})
            ));
          } else {
            let userIdFromFakeToken = authHeader.split(" ")[2];
            let body = createBody(connection.request.method, connection.request.url, testUsers);
            connection.mockRespond(new Response(
              new ResponseOptions({status: 200, body: {data: body}})
            ));
          }
        }

      }, 500);

    });

    return new Http(backend, options);
  },
  deps: [MockBackend, BaseRequestOptions]
};

function createBody(method: RequestMethod, url: string, testUsers: User[]) : any {
  if (method === RequestMethod.Get) {
    if (hasIdInUrl(url)) {
      return testUsers;
    }
    let requestedId = getIdFromUrl(url);
    return testUsers.filter(u => u.id === requestedId)[0];
  }
  if (method === RequestMethod.Put) {
    let requestedId = getIdFromUrl(url);
    // TODO: read request body update user in testUsers array and return changed user
    // TODO: should update return the changed user?
    return testUsers.filter(u => u.id === requestedId)[0];
  }
  if (method === RequestMethod.Delete) {
    let requestedId = getIdFromUrl(url);
    // TODO: should update return the removed user?
    return removeUserWithId(testUsers, requestedId);
  }
  // TODO: UserAdd ist noch nicht implementiert
  return {};
}

function getIdFromUrl(url: string) : number{
  return +url.split("/").pop();
}

function hasIdInUrl(url: string) : boolean {
  return url.split("/").length > 2;
}

function removeUserWithId(testUsers: User[], id: number) {
  let pos: number;
  for (let i = 0; i < testUsers.length; i++) {
    if (testUsers[i].id === id) {
      pos = i;
    }
  }
  let user = testUsers[pos];
  testUsers.splice(pos, 1);
  return user;
}