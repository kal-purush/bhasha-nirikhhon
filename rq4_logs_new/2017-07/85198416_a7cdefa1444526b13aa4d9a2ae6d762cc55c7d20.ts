import { AuthStoreBaseTests } from "../../shared/stores/AuthStoreBaseTests";
import { AuthStateBase, AuthStoreBaseInterface } from "../../../main/shared/stores/AuthStoreBase";
import { reportingAuthStore } from "../../../main/report/stores/ReportingAuthStore";

class ReportingAuthStoreTests extends AuthStoreBaseTests<AuthStateBase, AuthStoreBaseInterface<AuthStateBase>> {
    protected getStoreState(): AuthStateBase {
        return reportingAuthStore.getState();
    }

    protected expectedStateAfterLogin(token: string): AuthStateBase {
        return {
            loggedIn: true,
            permissions: [ "*/can-login", "*/other" ],
            username: "test.user",
            bearerToken: token
        };
    }

    protected initialState(): AuthStateBase {
        return {
            username: null,
            permissions: [],
            loggedIn: false,
            bearerToken: null
        };
    }

    protected storeName(): string {
        return "ReportingAuthStore";
    }

    protected getStore(): AuthStoreBaseInterface<AuthStateBase> {
        return reportingAuthStore;
    }
}

describe("ReportingAuthStore", () => {
    new ReportingAuthStoreTests().addTestsToMocha();
});