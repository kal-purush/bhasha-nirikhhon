import {it, describe, expect, inject} from "@angular/core/testing";
import {TestComponentBuilder} from "@angular/compiler/testing";
import {AmigoShareRequestCardComponent} from "./amigo-share-request-card.component";
import {AmigoShareRequest} from "../amigo-share-request/amigo-share-request";
import {DoseAmigosUser} from "../dose-amigos-user/dose-amigos-user";

/**
 * Tests for FeedEventCardComponent.
 */
describe("AmigoShareRequestCardComponent", () => {

    it("should render AmigoShareRequest as card", inject([TestComponentBuilder], (tcb: TestComponentBuilder) => {
        return tcb.createAsync(AmigoShareRequestCardComponent).then(componentFixture => {
            const element = componentFixture.nativeElement;

            componentFixture.componentInstance.amigoShareRequest = new AmigoShareRequest();
            componentFixture.componentInstance.amigoShareRequest.shareId = 1;
            componentFixture.componentInstance.amigoShareRequest.targetUserEmail = "a@example.org";

            componentFixture.componentInstance.amigoShareRequest.sharedAmigo = new DoseAmigosUser();
            componentFixture.componentInstance.amigoShareRequest.sharedAmigo.id = 100;
            componentFixture.componentInstance.amigoShareRequest.sharedAmigo.name = "John Doe";
            componentFixture.componentInstance.amigoShareRequest.sharedAmigo.lastTimeDoseTaken = new Date();
            componentFixture.componentInstance.amigoShareRequest.sharedAmigo.nextTimeDoseScheduled = new Date();

            componentFixture.detectChanges();
            expect(
                element.querySelectorAll("ion-card").length
            ).toBe(
                1,
                "should render AmigoShareRequest as card"
            );
        });
    }));

});