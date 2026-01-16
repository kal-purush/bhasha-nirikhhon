import { ClazzService } from './clazz.service';
import { CommonUserService } from './commonUser.service';
import { PermissionGroupService } from './permissionGroup.service';
import { PermissionGroupToBaseclassService } from './permissionGroupToBaseclass.service';
import { RoleService } from './role.service';
import { RoleToBaseclassService } from './roleToBaseclass.service';
import { SecurityLinkService } from './securityLink.service';
import { SecurityOperationService } from './securityOperation.service';
import { SecurityTenantService } from './securityTenant.service';
import { SecurityUserService } from './securityUser.service';
import { TenantToBaseclassService } from './tenantToBaseclass.service';
import { UserToBaseclassService } from './userToBaseclass.service';

export {
    PermissionGroupService, SecurityUserService, RoleService, SecurityTenantService, SecurityLinkService, ClazzService, SecurityOperationService,
    CommonUserService, UserToBaseclassService, RoleToBaseclassService, TenantToBaseclassService, PermissionGroupToBaseclassService
};
export const APIS = [PermissionGroupService, SecurityUserService, RoleService, SecurityTenantService, SecurityLinkService, ClazzService, SecurityOperationService,
    CommonUserService, UserToBaseclassService, RoleToBaseclassService, TenantToBaseclassService, PermissionGroupToBaseclassService];