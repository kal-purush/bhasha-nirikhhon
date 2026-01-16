/// <reference path="../all.d.ts" />

import {Room} from './room';
import {User} from './user';
import {Commands} from './commands';

export class Command {
	io: any;
	client: any;
	commands: Commands[];

	constructor(io: any, client: any) {
		this.commands = [];
		this.io = io;
		this.client = client;

		this.registerCommand('nick', 'Change nickname', (currentRoom: Room, user: User, args: string[]): string => {
			user.username = args[0];
			currentRoom.users[user.id].username = args[0];
			io.in(currentRoom.name).emit('renameUser', user.toJson());
			return '';
		});
	}

	registerCommand(name: string, description: string, callback: any) {
		this.commands[name] = new Commands(description, callback);
	}

	parseChat(currentRoom: Room, user: User, message: string): string {
		if (message.indexOf('/') == 0) {
			var args = message.substring(1).split(' ');
			if (this.commands[args[0]]) {
				message = this.commands[args[0]].callback(currentRoom, user, args.slice(1));
			} else {
				var error = 'unrecogised command: ' + message;
				this.client.emit('unrecogised', user, error);
				message = '';
				console.log(error);
			}
		}
		return message;
	}
}