// requires about\about.module.js
namespace appforgen.about {
  'use strict';

  interface IView {
    title: string;
  }
  export class AboutController implements IView {
    title: string = 'About';

    // static $inject: Array<string> = ['logger'];
    constructor() {
      console.log('lalala');
    }
  }

  angular
    .module('appforgen.about')
    .controller('AboutController', AboutController);
}