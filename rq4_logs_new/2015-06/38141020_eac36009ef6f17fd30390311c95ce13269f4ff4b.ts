/// <reference path="../typings/tsd.d.ts" />
/// <reference path="../typings/socket.io-client/socket.io-client.d.ts" />


import * as Rx from 'rx'
import io from 'socket.io-client'

let {
  Subject,
  Observable,
  Observer
} = Rx;

interface SailsConnectionEvent {}

interface SailsSocketResponse {
  
}

interface SailsSocketRequest {
  method: string;
  url: string;
  body: any;
  responseObserver: Rx.Observer<SailsSocketResponse>
}


interface SailsConnectionOptions {
  url: string;
  params: any;
  openObserver?:Rx.Observer<SailsConnectionEvent>,
  closingObserver?:Rx.Observer<SailsConnectionEvent>,
}

const buildSailsUrl = (host, params?) => {
  return `${host}?__sails_io_sdk_version=0.11.0`
}

export class RxSailsSocket {
  
  _socket: SocketIOClient.Socket;
  _subject: Rx.Subject<SailsSocketRequest | SailsSocketResponse>
  static create(url, options?:SailsConnectionOptions, openObserver?:Rx.Observer<SailsConnectionEvent>, closingObserver?:Rx.Observer<any>){
    
    return new RxSailsSocket(url, options, openObserver, closingObserver);
  }
  
  constructor(url, options?:SailsConnectionOptions, openObserver?:Rx.Observer<SailsConnectionEvent>, closingObserver?:Rx.Observer<any>){
    
    let socket:SocketIOClient.Socket;
    
    let disconnectSocket = (message?:string) => {
      if(socket){
        if(closingObserver){
          closingObserver.onNext(message);
          closingObserver.onCompleted();
        }
        console.log('closing sails connection...')
        socket.emit('disconnect');
      }
    }
    
    let observable = Observable.create(function(observer){
      
      socket = io(buildSailsUrl(url), options);
      
      let listeners = [];
      
      let connectHandler = connectionInfo => {
        if(openObserver){
          openObserver.onNext(connectionInfo);
          openObserver.onCompleted();
        }
      }
      
      socket.on('connect', connectHandler);
      
      
      return () => {
        disconnectSocket();
        socket.off('connect', connectHandler);
        
      }
    });
    
    let observer = Observer.create((request:SailsSocketRequest) => {
      console.log('sending request',request)
      if(socket && request.responseObserver){
        socket.emit(request.method, request, (response) => {
          request.responseObserver.onNext(response);
          request.responseObserver.onCompleted();
        });
      }
    });
    
    this._socket = socket;
    this._subject = Subject.create(observer, observable);
  }
  
  get(url, options){
     return Observable.create((obs) => {
       this._subject.onNext({
           method: 'get',
           url: url,
           options: options,
           responseObserver: obs
       });
     });
  }
}

window.Sails = RxSailsSocket;