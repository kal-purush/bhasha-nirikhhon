import {
    beforeEachProviders,
    describe,
    expect,
    inject,
    it,
} from "@angular/core/testing";
import {
    TestComponentBuilder,
} from "@angular/compiler/testing";

import { setBaseTestProviders } from "@angular/core/testing";
import {
    TEST_BROWSER_DYNAMIC_APPLICATION_PROVIDERS,
    TEST_BROWSER_DYNAMIC_PLATFORM_PROVIDERS,
} from "@angular/platform-browser-dynamic/testing";
import {DoseAmigosApp} from "./app";
import {Platform} from "ionic-angular/index";
import {provide} from "@angular/core";
import {FeedPage} from "./pages/feed/feed";

setBaseTestProviders(TEST_BROWSER_DYNAMIC_PLATFORM_PROVIDERS, TEST_BROWSER_DYNAMIC_APPLICATION_PROVIDERS);

class MockPlatform {
    ready(): Promise<string> {
        return Promise.resolve("core");
    }
}

/**
 * Tests for DoseAmigosApp.
 */
describe("DoseAmigosApp", () => {

    beforeEachProviders(() => [
        provide(Platform, {useClass: MockPlatform})
    ]);

    it("should have FeedPage as rootPage", inject([TestComponentBuilder], (tcb: TestComponentBuilder) => {
        return tcb.createAsync(DoseAmigosApp).then(componentFixture => {
            componentFixture.detectChanges();
            expect(
                componentFixture.componentInstance.rootPage
            ).toBe(
                FeedPage,
                "should have FeedPage as rootPage"
            );
        });
    }));

});