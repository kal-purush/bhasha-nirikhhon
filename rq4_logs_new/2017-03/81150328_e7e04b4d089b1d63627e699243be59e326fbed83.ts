import {ClothingItem} from "./clothing-item";

export class ClothingCombination {
  id: string;

  //Key: attribute ; Value: int 0-10 that represents the rate of the attribute within the clothingCombination
  attributes: {[attribute: string]: number;} = {};

  //Key: item type ; Value: item object
  items: {[item_type: string]: ClothingItem;} = {};

  //0 to 5 stars
  user_grade: number;

  //0 for not shown and 1 for already shown
  prv_shown: number;

  algorithm_grade: number;

  constructor(comb_id: string, comb_items: {}) {
    this.id = comb_id;
    this.items = comb_items;
  }

  get_id() {
    return this.id;
  }

  get_attributes() {
    return this.attributes;
  }

  get_items() {
    return this.items;
  }


  get_user_grade() {
    return this.user_grade;
  }

  get_prv_shown() {
    return this.prv_shown;
  }

  get_algorithm_grade() {
    return this.algorithm_grade;
  }

  edit_attributes(new_attributes: {}) {
    this.attributes = new_attributes;
  }

  edit_items(new_items: {}) {
    this.items = new_items;
  }


  edit_user_grade(new_user_grade: number) {
    this.user_grade = new_user_grade;
  }

  to_json() {
    return JSON.stringify(this);
  }

}