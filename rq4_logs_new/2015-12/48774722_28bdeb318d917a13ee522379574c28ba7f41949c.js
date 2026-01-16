Meteor.subscribe('releases');

Template.Releases.helpers({
  releases: ()=> {
    return Releases.find({});
  }
});