/// <reference path="./views/users/user-list-view" />
/// <reference path="./views/users/user-profile-view" />
/// <reference path="./core/collections/users" />
/// <reference path="./views/messages/message-list-view" />
/// <reference path="./core/collections/messages" />
/// <reference path="./core/models/user" />
/// <reference path="./utils/session" />

import UserListView = Views.Users.UserListView;
import UserProfileView = Views.Users.UserProfileView;
import UserCollection = Core.Collections.Users;

import MessageListView = Views.Messages.MessageListView;
import MessageCollection = Core.Collections.Messages;
import MessageModel = Core.Models.Message;

import UserModel = Core.Models.User;
import Session = Utils.Session;

// User Profile
let session = new Session();
let user:UserModel = session.login();

let userProfileView = new UserProfileView({
	model: user,
	el: '.user-profile',
	tagName: 'ul'
});

userProfileView.render();

// Contact List
let userCollection = new UserCollection([]);

let userListView = new UserListView({
	collection: userCollection,
	el: '.user-list',
	tagName: 'ul'
});

userListView.render();

userCollection.add(user);

// Message List
let messageCollection = new MessageCollection([]);

let messageListView = new MessageListView({
	collection: messageCollection,
	el: '.message-list',
	tagName: 'ul'
});

messageListView.render();

messageCollection.add(new MessageModel({
	text: 'hello there! :)',
	sender: user,
	datetime: new Date()
}));