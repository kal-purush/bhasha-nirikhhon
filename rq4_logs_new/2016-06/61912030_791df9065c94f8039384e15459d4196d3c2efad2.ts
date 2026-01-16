/// <reference path="./observ.d.ts" />
import { thunk, ThunkView } from '../lib/thunk';
import h = require('virtual-dom/h');
import ObservStruct = require('observ-struct');

interface IState {
}

class From extends ThunkView<IState> {
    dom(state: IState) {
        //return h('.ui.stufffff', 'welcome to stuff and things for real ok?');
        return h('pre', 'welcome to stuff and things for real ok? yes. i belive you now ;)');
    }
}

declare const document: any;
(function(){
    const f = new From(ObservStruct({}));
    document.body.appendChild(f.host());
}());