/// <reference path="../typings/angular2-meteor.d.ts" />
 
import {Projects} from '../collections/projects';

Meteor.publish('projects', function() {
    return Projects.find({
        $or: [
            { public: true },
            {
                $and: [
                    { owner: this.userId },
                    { owner: { $exists: true } }
                ]
            }
        ]
    });
});