import {Service} from './service';
import {SERVICES} from './mock-services';
import {Injectable} from 'angular2/core';

@Injectable()
export class ServiceNav {
  getServices() {
    return Promise.resolve(SERVICES);
  }
  // See the "Take it slow" appendix
  getServicesSlowly() {
    return new Promise<Service[]>(resolve =>
      setTimeout(()=>resolve(SERVICES), 2000) // 2 seconds
    );
  }
}

/*
Copyright 2016 Google Inc. All Rights Reserved.
Use of this source code is governed by an MIT-style license that
can be found in the LICENSE file at http://angular.io/license
*/