import * as b from 'node_modules/bobril/index';
import {Main} from './ui/main';

declare var uibench:any;

uibench.init('Bobril', '4.0.0');

document.addEventListener('DOMContentLoaded', function(e) {
  var container = document.querySelector('#App');
  let cache = null;
  uibench.run(
      function(state) {
          cache = b.updateChildren(container, Main(state), cache, null, null, 1000000);
      },
      function(samples) {
          cache = b.updateChildren(container, { tag: "pre", children: JSON.stringify(samples, null, ' ') }, cache, null, null, 1000000);
      }
  );
});