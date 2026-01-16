Outcoming Message
  GetContactList
  GetRecentMessages
  GetStarredMessages
  GetMessages
  SendMessage
  FindMessages
  ChangePointer
  StartTyping
  StarMessages
  DeleteMessages
  ClearChatHistory
  MarkChatAsRead

Incoming Messages
  StatusChanged
  PointerChanged
  ChatCreated
  ContactListChanged
  StartedTyping
  MessageReceived
  MessageDeleted
  ChatHistoryCleared

var OutcomingMessage = {
    GetContactList: function() {
        return [
            {
                id: new User(),
                chat: new Chat(),
                photo: "sdsds",
                login: "sdsd",
                name: "sdsd",
                status: 0,
                lastMessage: new Message(),
                unreadMessagesCount: 1
            }
        ]
    },
    GetRecentMessages: function(chat) {
        return {
            delivered: new Date(),
            seen: new Date(),
            messages: [new Message()]
        }
    },
    GetStarredMessages: function(beforeDate, count) {
        return [new Message()]
    },
    GetMessages: function(chat, date, direction, count) {
        return [new Message()]
    },
    SendMessage: function() {
        chat: new Chat(),
        text: "hello"
    },
    FindMessages: function(chat, starred, query) {
        return [new Message()]
    },
    ChangePointer: function(chat, type, date) {},
    StartTyping: function(chat, user) {},
    StarMessages: function(chat, dates) {},
    DeleteMessages: function(chat, dates) {},
    ClearChatHistory: function(chat) {},
    MarkChatAsReader: function(chat) {}
}

var IncomingMessage = {
    StatusChanged: {
        id: new User(),
        status: 1
    },
    PointerChanged: {
        chat: new Chat(),
        type: "delivered",
        date: new Date()
    },
    ChatCreated: {
        chat: new Chat(),
        users: [new User()]
    },
    ContactListChanged: {},
    StartedTyping: {
        chat: new Chat(),
        user: new User()
    },
    MessageReceived: new Date(),
    MessageDeleted: new Message(),
    ChatHistoryCleared: {
        date: new Date()
    }
}

var Message = {
    chat: new Chat(),
    sender: new User(),
    text: "sdsdsdsdsd",
    media: [new Media()],
    created_at: new Date(),
    starred: new Date()
}