/// <amd-dependency path="text!./error-dialog.html" />

import ko = require("knockout");
import cms = require("app/infrastructure");

export var template: string = require("text!./error-dialog.html");

export class viewModel extends cms.infrastructure.baseViewModel {
    constructor() {
        super();
        cms.infrastructure.shouter.subscribe(errorMessage=> {
            $('#dialogContent').text(<string>errorMessage);
            $('#genericDialog').modal({ keyboard: false, show: true });
        }, this, "showError");
    }
}