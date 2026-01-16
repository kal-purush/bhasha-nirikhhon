import {it, describe, expect, inject, beforeEachProviders} from "@angular/core/testing";
import {AuthUserService} from "./auth-user.service";
import {CURRENT_AUTH_USER} from "./auth-user.mocks";

/**
 * Tests for AuthUserService.
 */
describe("AuthUserService", () => {

    beforeEachProviders(() => [AuthUserService]);

    describe("get", () => {

        it("should return a AuthUser", inject([AuthUserService], (authUserService: AuthUserService) => {

            expect(
                authUserService.get()
            ).toEqual(
                CURRENT_AUTH_USER,
                "should return a AuthUser"
            );

        }));

    });

});