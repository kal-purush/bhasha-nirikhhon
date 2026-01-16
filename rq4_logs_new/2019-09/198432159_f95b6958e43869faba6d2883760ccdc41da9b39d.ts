import {LitElement} from 'lit-element';
import {Constructor} from '../../../types/globals';

function PermissionsMixin<T extends Constructor<LitElement>>(baseClass: T) {
  class PermissionsClass extends baseClass {
    hideEditIcon(isNew: boolean, editMode: boolean, canEdit: boolean) {
      if (isNew) {
        return true;
      }
      return !canEdit || editMode;
    }

    hideActionButtons(isNew: boolean, editMode: boolean, canEdit: boolean) {
      if (!canEdit) {
        return true;
      }
      if (isNew) {
        return false;
      }

      return !editMode;
    }

    isReadonly(editMode: boolean, canEdit: boolean) {
      return !(editMode && canEdit);
    }

  }
  return PermissionsClass;
}

export default PermissionsMixin;