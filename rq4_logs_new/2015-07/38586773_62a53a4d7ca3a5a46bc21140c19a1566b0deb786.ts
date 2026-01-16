/// <reference path="../../../server/all.d.ts" />

module irc {
	'use strict';

	export class Login {
		id: string;
		username:string;
		channel:string;

		constructor() {
			this.id = '';
			this.username = '';
			this.channel = ''
		}
	}
}