Template.contact.events({
  'submit .contact-form' : function() {
    console.log($(event.target).serialize());
  }
});