/**
 * Created by qiucheng on 15/5/14.
 */
function Conversation(customer) {
    this.customer = customer;
    this.messages = [];
    //this.type = type;
    this.lastReply = "";
    this.__selected = false;
    this.__unreadCount = 0;
}

Conversation.prototype.addReply = function (message) {
    this.lastReply = message;
    this.messages.push(message);

    if (!this.__selected)
        this.__unreadCount++;
}

Conversation.prototype.unselect = function () {
    this.__selected = false;
}

Conversation.prototype.select = function () {
    this.__selected = true;
    this.__unreadCount = 0;
}


module.exports = Conversation;