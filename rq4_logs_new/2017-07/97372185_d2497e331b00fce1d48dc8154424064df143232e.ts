import { Component } from '@angular/core';
import {MenuService} from '../../services/menu-service/menu.service';
import {MenuState} from '../../services/menu-service/menu-state.enum';

@Component({
  selector: 'side-menu',
  templateUrl: './side-menu.component.html',
  styleUrls: ['./side-menu.component.scss']
})
export class SideMenuComponent {

  currMenuState: MenuState;
  showMenu: boolean;
  menuStateEnum = MenuState;

  constructor(private menuService: MenuService) {
    this.menuService.getMenuState$().subscribe((menuState: MenuState) => {
      this.currMenuState = menuState;
      this.showMenu = this.shouldShowMenu();
    });
  }

  shouldShowMenu(): boolean {
    const statesArr = [MenuState.CLUSTER_LIST, MenuState.ESP_FORM]
    return statesArr.indexOf(this.currMenuState) > -1;
  }
}