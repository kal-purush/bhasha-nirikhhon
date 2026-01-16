import {
    Meteor
} from 'meteor/meteor';
import {
    check
} from 'meteor/check';
import {
    Accounts
} from 'meteor/accounts-base';
import {
    Csvdata,
    Users
} from '../../../both/collections/csvdata.collection';

export function reminderinvoice() {
    SyncedCron.add({
        name: 'Reminder function for invoice details',
        schedule: function(parser) {
            // parser is a later.parse object
            //('every 24 hours') or ('at 10:15 am') or ('every 1 mins')
            return parser.text('at 04:01 pm');
        },
        job: function() {
            var users;
            var userlist;
            users = Users.find({});
            users.subscribe((data) => {
                userlist = data;
                for (var i = 0; i < userlist.length; i++) {
                    if (Csvdata.find({
                            $and: [{
                                "Assigned_user_id": userlist[i]._id
                            }, {
                                "invoice_description": "invoice_description"
                            }]
                        }).cursor.count() > 9) {
                        Email.send({
                            "headers": {
                                'Content-Type': 'text/html; charset=UTF-8'
                            },
                            to: userlist[i].emails[0].address,
                            from: 'admin@excellencetechnologies.in',
                            subject: 'Reminder Fill invoice details ',
                            text: 'hi ' + userlist[i].username + ',<br><br>  I’m sending you this email as a reminder to fill remaining invoice details.<br><br>Thanks'
                        });
                    }
                }
            });
        }
    });

    SyncedCron.start();
}