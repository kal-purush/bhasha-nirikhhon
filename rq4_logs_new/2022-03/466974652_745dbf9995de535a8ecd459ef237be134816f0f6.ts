import { Injectable } from '@angular/core';
import User from './User';

@Injectable({
	providedIn: 'root'
})
export class UsersService {

	private _users: User[] = [{
		id: 1,
		firstName: "Ana",
		lastName: "Popa",
		email: "ana.popa@bearingpoint.com",
		username: "ana.popa"
	},
	{
		id: 2,
		firstName: "Andreea",
		lastName: "Bucur",
		email: "andreea.bucur@bearingpoint.com",
		username: "andreea.bucur"
	}];

	private idCounter: number = 3;

	constructor() { }

	public get users(): User[] {
		return this._users;
	}

	public set users(users: User[]) {
		this._users = users;
	}

	public addUser(user: User): void {
		user.id = this.idCounter;
		this._users.push(user);
		this.idCounter++;
	}

	public findUserById(id: number): User {
		for (let user of this.users) {
			if (user.id == id) {
				return user;
			}
		}
		return new User();
	}

	public editUser(id: number, firstName: string, lastName: string, email: string): void {
		const userToEdit: User | null = this.findUserById(id);
		if (!userToEdit) {
			return;
		}
		if (userToEdit) {
			userToEdit.firstName = firstName;
			userToEdit.lastName = lastName;
			userToEdit.email = email;
			userToEdit.username = email.split('@')[0];
		}
	}

	public deleteUsers(selectedUsers: User[]): void {
		for (let user of selectedUsers) {
			this.deleteUser(user);
		}
	}

	deleteUser(user: User) {
		this.users.splice(this.users.indexOf(user), 1);
	}

}