import { Component, OnInit, ViewEncapsulation } from "@angular/core";
import { FuseConfigService } from '@fuse/services/config.service';
import { $ } from 'protractor';
// import * as totalscript from '../../../assets/js/totalscript.js';

declare var totalscript: any;

@Component({
  selector: "app-welcome",
  templateUrl: "./welcome.component.html",
  styleUrls: ["./welcome.component.scss"],
  encapsulation: ViewEncapsulation.None,
})

export class WelcomeComponent implements OnInit {
  
  
  constructor(
    private _fuseConfigService: FuseConfigService,
  ) { 
    this._fuseConfigService.config = {
      layout: {
          navbar   : {
              hidden: true
          },
          toolbar  : {
              hidden: true
          },
          footer   : {
              hidden: true
          },
          sidepanel: {
              hidden: true
          }
      }
  };
  }

  ngOnInit() {
  
    totalscript();
    // $(window).load(
    //   function(){}
    // );

  }

  
  
}