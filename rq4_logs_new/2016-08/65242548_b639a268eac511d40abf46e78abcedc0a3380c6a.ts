import { Component } from '@angular/core';
import {SideBarService} from './sidebar.service';


export interface MaterialComponentHandler {
  upgradeDom();
};

declare var componentHandler: MaterialComponentHandler;

@Component({
  moduleId: module.id,
  selector: 'app-sidebar',
  host: { class: 'demo-drawer mdl-layout__drawer mdl-color--blue-grey-900 mdl-color-text--blue-grey-50' },
  templateUrl: 'sidebar.component.html',
  styleUrls: ['sidebar.component.css'],
  providers: [SideBarService]



})

export class SidebarComponent {

  allSelected: boolean;
  partialSelected: boolean;
  myData: any;
  objectArray = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
  listArray=[
    {"item": "Account"},
    {"item": "money"},
            
  ];

    selectAll(event){
    this.allSelected = !this.allSelected;
    alert(this.allSelected);

  }
  
  SelectPart(event){
    this.partialSelected = !this.partialSelected;
    alert(this.partialSelected);
  }


  constructor(private sidebarService: SideBarService) { }

  ngOnInit() {
    this.sidebarService.getData()
      .subscribe(data => this.myData = data);
  }

}
