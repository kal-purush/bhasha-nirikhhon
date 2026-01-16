package uk.gov.hmcts.reform.amlib.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.jdbi.v3.core.mapper.reflect.JdbiConstructor;
import uk.gov.hmcts.reform.amlib.enums.Permission;
import uk.gov.hmcts.reform.amlib.internal.utils.Permissions;

import java.util.Set;

@Data
@Builder
@AllArgsConstructor
public class DefaultRolePermissions {
    private String role;
    private Set<Permission> permissions;

    @JdbiConstructor
    public DefaultRolePermissions(String role, int permissions) {
        this.role = role;
        this.permissions = Permissions.fromSumOf(permissions);
    }
}