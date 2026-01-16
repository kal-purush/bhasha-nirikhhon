import {Module} from "../../models/module";

export class CategoryElement {
  position: number;
  name: string;
  module: Module;
  isCustom: boolean; //says if category is added by user or by system
  isEditing: boolean;
  isNew: boolean;
}