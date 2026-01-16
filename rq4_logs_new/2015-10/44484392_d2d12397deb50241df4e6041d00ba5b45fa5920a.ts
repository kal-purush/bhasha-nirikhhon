/// <reference path="../../typings/tsd.d.ts" />

import {
  Component,
  OnInit,
  NgFor
} from 'angular2/angular2';

import {FolderCmp} from 'client/folder/folder_cmp.js';

@Component({
  selector: 'folder-list-cmp',
  template: `
    <folder-cmp [folder-name]=" 'folder_1' "></folder-cmp>
    <folder-cmp [folder-name]=" 'folder_2' "></folder-cmp>
    <folder-cmp [folder-name]=" 'folder_3' "></folder-cmp>
  `,
  directives: [FolderCmp]
})
export class FolderListCmp implements OnInit {
  constructor() {

  }

  onInit() {
    console.log('folder-list-cmp init');
  }
}