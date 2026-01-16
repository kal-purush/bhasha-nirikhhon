import {it, describe, expect, inject} from "@angular/core/testing";
import {TestComponentBuilder} from "@angular/compiler/testing";
import {FeedEventCardComponent} from "./feed-event-card.component";
import {FeedEvent} from "../feed-event/feed-event";
import {DoseAmigosUser} from "../dose-amigos-user/dose-amigos-user";

/**
 * Tests for FeedEventCardComponent.
 */
describe("FeedEventCardComponent", () => {

    it("should render FeedEvent as card", inject([TestComponentBuilder], (tcb: TestComponentBuilder) => {
        return tcb.createAsync(FeedEventCardComponent).then(componentFixture => {
            const element = componentFixture.nativeElement;

            componentFixture.componentInstance.feedEvent = new FeedEvent();
            componentFixture.componentInstance.feedEvent.id = 1;
            componentFixture.componentInstance.feedEvent.time = new Date();

            componentFixture.componentInstance.feedEvent.user = new DoseAmigosUser();
            componentFixture.componentInstance.feedEvent.user.id = 100;
            componentFixture.componentInstance.feedEvent.user.name = "John Doe";
            componentFixture.componentInstance.feedEvent.user.lastTimeDoseTaken = new Date();
            componentFixture.componentInstance.feedEvent.user.nextTimeDoseScheduled = new Date();

            componentFixture.detectChanges();
            expect(
                element.querySelectorAll("ion-card").length
            ).toBe(
                1,
                "should render FeedEvent as card"
            );
        });
    }));

});