Template['contact'].helpers({
    'errorEmail': () => {
        if (Session.get('error-email')) {
            return "has-error";
        }
    },
    'errorName': () => {
        if (Session.get('error-name')) {
            return "has-error";
        }
    },
    'errorPosition': () => {
        if (Session.get('error-position')) {
            return "has-error";
        }
    },
    'errorPrice': () => {
        if (Session.get('error-price')) {
            return "has-error";
        }
    },
    'errorSelection': () => {
        if (Session.get('error-selection')) {
            return "has-error";
        }
    },
    'feedback': () => {
        return Session.get('contact-feedback');
    }
});

Template['contact'].events({
    'click .contact-btn': () => {
        var $contact = $('.contact');

        if ($contact.is(':visible')) {
            $contact.hide(300);
        } else {
            $contact.show(300);
        }
    },

    'submit .new-shop': (e) => {
        var email = e.target.email.value
        var name = e.target.name.value
        var position = e.target.position.value
        var price = e.target.price.value
        var selection = e.target.selection.value

        var hasErrors = false;

        _.each([
            { name: "email", value: email },
            { name: "name", value: name },
            { name: "position", value: position },
            { name: "price", value: price },
            { name: "selection", value: selection }
        ], (field) => {
            if (field.value == '') {
                hasErrors = true;
                Session.set('error-' + field.name, true);
                console.log('error-' + field.name);
            } else {
                Session.set('error-' + field.name, false);
            }
        });

        if (hasErrors) {
            Session.set('contact-feedback', 'Du skal fylde det hele ud jo');
            return false;
        }

        var values = {
            name: name,
            position: position,
            price: price,
            selection: selection
        };

        Meteor.call('submitShop', email, values, (error, result) => {
            if (!!error) {
                console.error(error);
                Session.set('contact-feedback', 'Det har sked en fejl :\'-(');

            } else {
                console.log('result: ' + result);

                e.target.reset();
                Session.set('contact-feedback', 'Jamand, tak for det!');
            }
        });

        return false;
    }
});