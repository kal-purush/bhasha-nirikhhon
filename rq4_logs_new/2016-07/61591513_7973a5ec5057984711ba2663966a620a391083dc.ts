import {it, describe, expect} from "@angular/core/testing";
import {NavController} from "ionic-angular/index";
import {NewAmigoRequestPage} from "./new-amigo-request";

class MockNavController {
    public pop() {

    }
}

/**
 * Tests for NewAmigoRequestPage component.
 */
describe("NewAmigoRequestPage", () => {

    it("should have title set", () => {

        const mockNavController: NavController = new MockNavController() as NavController;

        const newAmigoRequestPage: NewAmigoRequestPage = new NewAmigoRequestPage(
            mockNavController
        );

        return newAmigoRequestPage.ngOnInit().then(
            () => {
                expect(
                    newAmigoRequestPage.title
                ).toEqual(
                    "New Amigo Request",
                    "should have title set"
                );
            }
        );

    });

});