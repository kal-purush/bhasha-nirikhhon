

/*
  This program and the accompanying materials are
  made available under the terms of the Eclipse Public License v2.0 which accompanies
  this distribution, and is available at https://www.eclipse.org/legal/epl-v20.html
  
  SPDX-License-Identifier: EPL-2.0
  
  Copyright Contributors to the Zowe Project.
*/


import { Inject, Component, ElementRef, OnInit, ViewEncapsulation, OnDestroy, ViewChild, NgModule } from '@angular/core';
import { CommonModule } from "@angular/common";
import { Observable, Subject } from 'rxjs';
import { ComponentClass } from '../../../../../../zlux-platform/interface/src/registry/classes';
import { UtilsService } from '../../services/utils.service';
import { Capability, FileBrowserCapabilities } from '../../../../../../zlux-platform/interface/src/registry/capabilities';
import { ZluxGridModule, ZluxGridComponent, ZluxTableMetadataToColumnsPipe } from '@zlux/grid';
import { Angular2InjectionTokens, Angular2PluginWindowActions, Angular2PluginWindowEvents } from 'pluginlib/inject-resources';

@Component({
  selector: 'gridcomp',
  templateUrl: 'gridcomp.component.html',
  encapsulation: ViewEncapsulation.None,

  providers: [UtilsService]
})

export class gridcomp {
  private input_box: string;


  private showGrid: boolean = false;
  private columnMetaData: any = null;
  private rows: any = null;

  @ViewChild('grid') grid;


  constructor(private utilsService: UtilsService, @Inject(Angular2InjectionTokens.WINDOW_ACTIONS) private windowAction: Angular2PluginWindowActions,
    @Inject(Angular2InjectionTokens.WINDOW_EVENTS) private windowEvents: Angular2PluginWindowEvents) {
    this.input_box = "/";
  }

  updateGrid(): void {
    let columnMeta = {
      columnMetaData: [
        {
          "columnIdentifier": "name",
          "longColumnLabel": "name",
          "rawDataType": "string"
        },
        {
          "columnIdentifier": "directory",
          "longColumnLabel": "directory",
          "rawDataType": "string"
        },
        {
          "columnIdentifier": "size",
          "longColumnLabel": "size",
          "rawDataType": "number"
        },
        {
          "columnIdentifier": "createdAt",
          "longColumnLabel": "createdAt",
          "rawDataType": "number"
        },
        {
          "columnIdentifier": "mode",
          "longColumnLabel": "mode",
          "rawDataType": "number"
        },
      ]
    }


    this.utilsService.getDataset("/unixFileContents" + this.input_box)
      .subscribe(data => {
        console.log("column");
        console.log(data.metaData);
        console.log("row");
        console.log(data.entries.map(x => Object.assign({}, x) ));
        this.columnMetaData = columnMeta;
        this.rows = data.entries.map(x => (({name, directory, size, createdAt, mode}) => ({name, directory, size, createdAt, mode}))(x));
        this.showGrid = true;
      },
      error => {
        this.showGrid = false;
      });
  }



}



/*
  This program and the accompanying materials are
  made available under the terms of the Eclipse Public License v2.0 which accompanies
  this distribution, and is available at https://www.eclipse.org/legal/epl-v20.html
  
  SPDX-License-Identifier: EPL-2.0
  
  Copyright Contributors to the Zowe Project.
*/
