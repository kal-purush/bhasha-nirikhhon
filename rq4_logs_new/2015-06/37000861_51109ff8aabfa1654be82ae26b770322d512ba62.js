Template.messages.helpers({
  messages: [
    { text: "All these messages" },
    { text: "Uses the same template" },
    { text: "But have a different data context" },
    { text: "It's why these messages are all different!" },
    { text: "Hey!" },
    { text: "What's up man!" },
    { text: "Hello" }
  ]
});

Template.footer.events({
  'keypress input': function(event) {
        if (event.charCode == 13) {
            event.stopPropagation();
            $('.message-history').append('<div class="message"><a href="" class="message_profile-pic"></a><a href="" class="message_username">federico</a><span class="message_timestamp">1:31 AM</span><span class="message_star"></span><span class="message_content">' + $('.input-box_text').val() + '</span></div>');
            $('.input-box_text').val("");
            return false;
        }
    }
});