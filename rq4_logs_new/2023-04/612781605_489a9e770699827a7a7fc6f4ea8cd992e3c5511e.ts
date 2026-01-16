import { Component, Input, OnDestroy, OnInit } from '@angular/core';
import { Subscription } from 'rxjs';
import { Folder } from 'src/app/interfaces/folder.interface';
import { FolderTreeService } from '../folder-tree.service';

@Component({
  selector: 'app-folder-node',
  templateUrl: './folder-node.component.html',
  styleUrls: ['./folder-node.component.less']
})
export class FolderNodeComponent implements OnInit, OnDestroy {
  folderSubscription: Subscription;
  activeFolder: Folder;

  constructor(private folderService: FolderTreeService) { }

  ngOnInit(): void {
    if(!this.folder.path) {
      if(!this.path) {
        this.folder.path = this.folder.name
      } else {
        this.folder.path = `${this.path} / ${this.folder.name}`;
      }
    }

    this.folderSubscription = this.folderService.folderSource.subscribe(folder => {
      this.activeFolder = folder
    })
  }

  expand: boolean = false

  @Input() folder: Folder;
  @Input() path: string;

  toggleFolder(){
    this.expand = !this.expand
  }

  chooseFolder(){
    this.folderService.folderSource.next(this.folder)
  }

  ngOnDestroy(): void {
    this.folderSubscription.unsubscribe();
  }

}