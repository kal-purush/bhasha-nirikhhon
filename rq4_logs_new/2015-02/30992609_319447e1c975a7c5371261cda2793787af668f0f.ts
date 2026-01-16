/// <reference path="../../_references.d.ts" />

import plat = require('platypus');
import BaseRepository = require('../base/base.repository');
import UserService = require('../../services/user/user.service');

class UserRepository extends BaseRepository {
	userid: number;
	email: string;

	constructor(private userService: UserService) {
		super();
	}

	login(email: string, password: string) : plat.async.IThenable<boolean> {
		return this.userService.login(email, password).then((user) => {
			this.userid = user.id;
			this.email = user.email;
			return true;
		}).catch((error) => {
			return false;
		});
	}
}

plat.register.injectable('user-repository', UserRepository, [UserService]);

export = UserRepository;