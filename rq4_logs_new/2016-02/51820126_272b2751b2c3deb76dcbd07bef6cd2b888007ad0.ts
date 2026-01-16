import {Http, Headers, RequestOptionsArgs} from 'angular2/http';
import {Subject, Observable} from 'rxjs'
import {Injectable} from 'angular2/core';

// idea is to make a layer on top of Http which is transparent to the user but comes with this additional features:
// - connection tracking
//      observable keeps track of its connection status. if one call fails (timesout or host unreachable) it will mark connection as down
//      and will not try to make another call until connection is back up or ping_delay ms pass
//      it can also do periodic checks on connection (if ping_url is provided)
//  - exposes connection status
//  - guarantees that request will be executed once the connection is back (in case its down)
//  - exposes the list of requests in progress and allows setting the list
//      which allows us to store its state in localStorage and then restore it next time application loads
//
// this way the consumer should have less problems writting application that will also work offline
// when user goes temporary offline, he would continue to use the application
// application would notify him about the offline status (binding to the getStatus() of this service)
// some UI features could also be disabled based on this status
//
// when he would normally execute http.get and get an error, here he would be sure that once connection is back his data will be updated
// he would still get error notification, when he could inform the user that the data he is seeing could not be up to date due to offline status
//
// this comes to full power with update put and delete request. we can work offline, creating new entries (lets say in our ToDo app)
// when we come back online all data will be sent to the server in the same order as we did the actions.
//
// we can provide the user with information about how many requests are still pending and for example when he tries to leave the pge
// we could notify him about pending requests and offer him an option to save them and send them later, or discard them
@Injectable()
export class HttpQueue  {

    // todo: expose requestList (or another map) as observable (maybe just the length ?)
    private requestList: any;                   // list of requests in progress
    private requestListMap: any;                // map to allow to quickly find requests by method + url

    public ping_url: string;
    // todo: expose status as observable
    private ping_status: boolean;               // status of connection: true (online) or false (offline)
    private ping_timestamp: any;                // last time ping was executed
    private ping_delay: any;                    // delay between ping requests

    // ping_url : url to ping to check connection (should be a resource on the server which is very small (1 byte?)
    constructor(private http: Http) {
        this.ping_delay = 1*20*1000;
        this.ping_timestamp = Date.now();
        this.ping_status = true;
        this.requestList = {};
        this.requestListMap = {};
    }


    // pings the host to check connection status
    // force parameter: will check right away (without waiting for DELAY)
    ping(force: boolean = false) {
        if (!force && Date.now() < this.ping_timestamp + this.ping_delay) return;

        this.ping_timestamp = Date.now();

        // if url is not provided we set status back to true after DELAY and call the send
        if (!this.ping_url) {
            this.ping_status = true;
            this.send();
            return;
        }

        // send the request and subscribe to it
        this.http.get(this.ping_url).subscribe(req => {
            // if connection came back (ping status was false before) then we need to call send to send any waiting requests
            if (!this.ping_status) {
                this.ping_status = true;
                this.send();
            }
            this.ping_status = true;
        }, err => {
            this.ping_status = false;
        });
    }

    // monitors web connection
    // runs ping every DELAY ms
    // run parameter: will run ping even if ping status is true (online)
    monitor(run: boolean = false) {
        this.ping();
        if (!this.ping_status || run) {
            var self = this;
            setTimeout(function() {
                self.monitor();
            }, this.ping_delay);
        }
    }

    // send the requests in queue
    // todo: we take first request, execute it and only after its complete we execute the next one
    // this way if the first request fails we dont run all the others
    send() {
        for (var x in this.requestList) {
          // here we need to resend the old requests
          var req = this.requestList[x];
          var http_req;

          // create http request
          if (req.method == "post" || req.method == "put") http_req = this.http[req.method](req.url, req.data, req.options);
          else http_req = this.http[req.method](req.url, req.options);

          http_req.subscribe(data => {
            console.log('request succeded');
            // we should set ping status here and cancel any pings in progress
            // this.ping_status = true;
            // remove request from the queue
            delete this.requestListMap[req.method + req.url];
            delete this.requestList[x];

            this.ping_timestamp = Date.now();

            // notify subscribers
            if (req.observer) req.observer.next({status: 'OK', data: data });
          }, err => {
            console.log('request failed');
            // if connection failed and resend is false delete from queue and notify client about error
            if (req.options.resend === false) {
              // todo: throw error on this.requestListpx[x[.observable
              delete this.requestListMap[req.type + req.url];
              delete this.requestList[x];
            }

            // set status and run the monitor to periodically check for online status
            // todo: call monitor asynchronously
            this.ping_timestamp = Date.now();
            this.ping_status = false;
            this.monitor();

            // notify client subscribers of failed request
            if (req.observer) req.observer.next({status: 'FAILED', data: err});
          }, () => {
            console.log("request completed");
          });

        }
    }

    // dont return http.get observable, create another one and return that one
    // errors are wrapped in the message (doesnt throw error on http error, but gives a normal message)
    // this way observable will not terminate and we can later send a valid data, once the request succeds
    call(method: string, url: string, data: string, options: any = {}) {
      // observable we return to the client
      // obs is our observer to which we send data
      var req, ts = Date.now();

      // default: dont resend GET requests
      if (!options.resend) options.resend = method == 'get' ? false: true;

      // check if same request already exists in the queue, but skip the check for post and put
      // todo: put should actually update the data of the request in the queue
      if (["post", "put"].indexOf(method) == -1 && this.requestListMap[method + url]) {
        ts = this.requestListMap[method + url];
        req = this.requestList[ts].request;
      }

      if (!req) {
        // create observable to return to the caller
        req = new Observable(observer => {
          this.requestList[ts].observer = observer;
        }).share();

        // add request to the queue and create a map to easily find it by method + url
        this.requestList[ts] = {
          method: method,
          url: url,
          data: data,
          options: options,
          request: req
        };
        this.requestListMap[method + url] = ts;
      }

      // if connection is down we should not send the request
      // todo: call this asynchronously
      if (this.ping_status) this.send();

      return req;
    }

    // sample calls:
    // if we currently have no connection error will be emited every PING_DELAY miliseconds, once connection is back this.data will be updated
    // myHttp.get('/myurl').map(req => req.json()).subscribe(data => { this.data = data; }, error => { console.log("no connection atm ..."); } );
    //
    // if later another client tries to make same request, and there is already one request in progress (lets say connection is down and request is waiting)
    // he will subscribe to the same observable (another request will never be created)
    // myHttp.get('/myurl').map(req => req.json()).subscribe(data => { this.data2 = data; });
    //
    // if we start pooling (Observable.interval(1000).mergeMapLatest(myHttp.get('/')).subscribe(data=> {})
    // it should still work ... basicly we should be able to do anything with myHttp.get we are able to do with http.get
    // (should be completely transparent)
    get(url: string, options: any = {}) {
        return this.call('get', url, null, options);
    }

    // if another client makes another post request later, we should send both
    post(url: string, body, options: any = {}) {
        return this.call('post', url, body, options);
    }

    // if another client makes another delete request we send just one
    delete(url: string, options: any = {}) {
        return this.call('delete', url, null, options);
    }

    // if another client tries to make another put request we send both
    put(url: string, body, options: any = {}) {
        return this.call('put', url, body, options);
    }

    request(url: string, options: any = {}) {
        return this.call('request', url, null, options);
    }

    // saving and loading the queue
    // when saving we remove data we wont need later (observers will need to be registered again)
    saveQueue() {
      var requestList = {}
      for (var x in this.requestList) {
        requestList[x] = this.requestList[x];
        delete requestList[x].observer;
        delete requestList[x].request;
      }
      localStorage.setItem("myhttp_queue", JSON.stringify(requestList));
    }

    loadQueue() {
      this.requestList = JSON.parse(localStorage.getItem("myhttp_queue"));
    }
}