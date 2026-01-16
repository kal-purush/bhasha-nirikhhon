import {Injectable} from 'angular2/core';
import {AppStore} from 'angular2-redux';

import {socket} from '../../../socket';
import {ConsulActions} from '../../../actions/consul.actions';

@Injectable()
export class NodesService {

  constructor (private _appStore: AppStore,
               private _consulActions: ConsulActions) {
    var vm = this;

    socket.onmessage = function(e) {
      //console.log('on message:', e);
      var d = JSON.parse(e.data);
      if (d.from === 'consul' && d.type === 'nodes') {
        vm._appStore.dispatch(vm._consulActions.receiveNodes(null, d.data));
      }
    };
  }

}