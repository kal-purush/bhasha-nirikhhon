import { createItemDto } from 'src/api/items/DTO/create-item.dto';

export const itemValidations = (item: createItemDto) => {
  if (
    !item.name ||
    !item.type ||
    !item.schoolId ||
    !item.inStock ||
    item.ordered === undefined ||
    item.isTemporal === undefined
  ) {
    return false;
  }
  return true;
};