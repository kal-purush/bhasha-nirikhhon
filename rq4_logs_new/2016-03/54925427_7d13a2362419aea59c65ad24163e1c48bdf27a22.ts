/**
 * ContactListController
 */
class ContactListController {
    static $inject: Array<string> = [];
    constructor() {
        var vm = this;
    }
}

export function ContactListDirective(): ng.IDirective {
    return {
        restrict: 'E',
        templateUrl: '/build/components/contact-list/contact-list.html',
        controller: ContactListController,
        controllerAs: 'contactListCtrl',
        scope: {
            "items": "="
        },
        bindToController: true
    };
}