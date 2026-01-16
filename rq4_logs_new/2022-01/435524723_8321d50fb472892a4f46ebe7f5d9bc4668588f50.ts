import React from 'react';

import { ReactComponent as FoodIcon } from '../images/icons/food.svg';
import { ReactComponent as MedicineIcon } from '../images/icons/medicine.svg';
import { ReactComponent as WaterIcon } from '../images/icons/water.svg';
import { ReactComponent as WeaponIcon } from '../images/icons/weapon.svg';
import { ReactComponent as UnknownIcon } from '../images/icons/unknown.svg';
import { ItemIconMap, ItemIconType } from '../models/shared';



export const ITEM_ICON_MAP: ItemIconMap = {
  [ItemIconType.Food]: FoodIcon,
  [ItemIconType.Medicine]: MedicineIcon,
  [ItemIconType.Water]: WaterIcon,
  [ItemIconType.Weapon]: WeaponIcon,
};

export function getItemIcon(itemType: string | ItemIconType): React.FunctionComponent {
  return ITEM_ICON_MAP[itemType] !== undefined ? ITEM_ICON_MAP[itemType] : UnknownIcon;
}