import { IconType } from 'types';
import { JsonModel, JsonModelProperty } from 'typescript-json-model';

export interface NavbarItemObject {
  id: number;
  title: string;
  link: string;
  icon: IconType;
}

export class NavbarItem extends JsonModel<NavbarItemObject> implements NavbarItemObject {
  @JsonModelProperty
  public id: number;
  @JsonModelProperty
  public title: string;
  @JsonModelProperty
  public link: string;
  @JsonModelProperty
  public icon: IconType;

  constructor(navbarItem: NavbarItemObject) {
    super(navbarItem);
    this.id = navbarItem.id;
    this.title = navbarItem.title;
    this.link = navbarItem.link;
    this.icon = navbarItem.icon;
  }
}