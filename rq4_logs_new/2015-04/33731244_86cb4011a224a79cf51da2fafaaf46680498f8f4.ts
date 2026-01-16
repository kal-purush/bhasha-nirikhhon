/// <amd-dependency path="xomni" />
/// <amd-dependency path="text!./msg-integration.html" />

import $ = require("jquery");
import ko = require("knockout");
import cms = require("app/infrastructure");

export var template: string = require("text!./msg-integration.html");

export class viewModel extends cms.infrastructure.baseViewModel {
    public client = new Xomni.Management.Integration.MSG.MSGClient();
    public isEnabled = ko.observable(false);
    public email = ko.observable<string>().extend({ required: true, email: true });
    public firstName = ko.observable<string>().extend({ required: true });
    public lastName = ko.observable<string>().extend({ required: true });
    public ssoUrl = ko.observable<string>();
    public password = ko.observable<string>();
    public validationErrors = ko.validation.group([this.email, this.firstName, this.lastName]);

    constructor() {
        super();
        this.initalize();
    }

    initalize() {
        this.client.get(
            (t) => {
                this.email(t.Email);
                this.ssoUrl(t.SsoUrl);
                this.isEnabled(true);
            },
            (e) => {
                if (e.HttpStatusCode == 404) {
                    this.isEnabled(false);
                }
                else {
                    this.showErrorDialog();
                }
            }
            );
    }

    enableIntegration() {
        if (this.validationErrors().length == 0) {
            this.client.post({
                Email: this.email(),
                FirstName: this.firstName(),
                LastName: this.lastName()
            },
                (t) => {
                    this.password(t.Password);
                    this.email(t.Email);
                    this.ssoUrl(t.SsoUrl);
                    this.isEnabled(true);
                },
                (e) => {
                    if (e.HttpStatusCode == 400) {
                        this.showCustomErrorDialog("Please enable integration endpoint first.");
                    }
                    else {
                        this.showErrorDialog();
                    }
                }
                );
        }
        else {
            this.validationErrors.showAllMessages();
        }
    }
}