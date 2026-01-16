import { CanActivate, Component } from "../../../libs/component-decorators";

@Component({
    route: "/customer/edit/:id",
    templateUrl: "wwwroot/components/customer/customer-editor.html",
    selector: "customer-editor",
    providers: ["$location", "customerActionCreator", "invokeAsync"]
})
export class CustomerEditorComponent { }