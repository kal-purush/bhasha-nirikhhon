package skillfactory.DreamTeam.globus.it.config.security;

import org.springframework.security.access.expression.SecurityExpressionRoot;
import org.springframework.security.access.expression.method.MethodSecurityExpressionOperations;
import org.springframework.security.core.Authentication;
import skillfactory.DreamTeam.globus.it.dto.auth.WalletUserDetails;

import java.util.Objects;

public class WalletExpressionRoot extends SecurityExpressionRoot implements MethodSecurityExpressionOperations {

    private Object filterObject;
    private Object returnObject;

    public WalletExpressionRoot(Authentication authentication) {
        super(authentication);
    }

    public boolean hasAccount(Long accountId) {
        WalletUserDetails userDetails = (WalletUserDetails) getPrincipal();
        if (userDetails == null || userDetails.getAccountId() == null) return false;
        return Objects.equals(userDetails.getAccountId(), accountId);
    }

    public boolean hasUser(Long userId) {
        WalletUserDetails userDetails = (WalletUserDetails) getPrincipal();
        if (userDetails == null || userDetails.getUserId() == null) return false;
        return Objects.equals(userDetails.getUserId(), userId);
    }

    @Override
    public void setFilterObject(Object filterObject) {
        this.filterObject = filterObject;
    }

    @Override
    public Object getFilterObject() {
        return filterObject;
    }

    @Override
    public void setReturnObject(Object returnObject) {
        this.returnObject = returnObject;
    }

    @Override
    public Object getReturnObject() {
        return returnObject;
    }

    @Override
    public Object getThis() {
        return this;
    }
}