/// <reference path="../all.d.ts" />

interface  IRoom {
	name: string;
	id: number;
	owner: IUser;
	users: IUser[];
	private: boolean;

	addUser(user: IUser): void;
	getUser(id: number): IUser;
	getListUser(): IUser[];
	isPrivate(): boolean;
	removeUser(id: number): void;
}

class Room implements IRoom{
	name: string;
	id: number;
	owner: IUser;
	users: IUser[];
	private: boolean;

	constructor(name: string, id: number, owner: IUser) {
		this.name = name;
		this.id = id;
		this.owner = owner;
	}

	addUser(user: IUser): void {
		this.users.push(user);
	}

	getUser(id: number): IUser {
		this.users.forEach((element: IUser, index: number) => {
			if(element.id == id) {
				return element;
			}
		});
		return undefined;
	}

	getListUser(): IUser[] {
		return this.users;
	}

	isPrivate(): boolean {
		return this.private;
	}

	removeUser(id: number): void {
		this.users.forEach((element: IUser, index: number) => {
			if(element.id == id) {
				this.users.splice(id, 1);
			}
		});
	}
}