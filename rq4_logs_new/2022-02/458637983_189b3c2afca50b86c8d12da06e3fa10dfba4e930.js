import { Confirmation } from '../src/confirmation'

describe('The component with Bootstrap style', function () {
    it("should have bootstrap-classes applied to the buttons", function () {
        let fakeButton = document.createElement("button");
        let config = { style: "bootstrap" };
        let instance = new Confirmation(fakeButton, config);

        fakeButton.click();
        let yesButton = document.querySelector(".ays-yes.btn.btn-success");
        expect(yesButton).not.toBeNull();

        let noButton = document.querySelector(".ays-no.btn.btn-danger");
        expect(noButton).not.toBeNull();
    });
});