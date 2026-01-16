'use strict';

import Base from './base.js';

export default class extends Base {
    async staticpermissionAction() {
        let permName = this.get("permission");
        let permissionModel = this.model('permissions');
        let result = await permissionModel.getPermissionInfoByName(permName);
        return this.success(result);
    }
}