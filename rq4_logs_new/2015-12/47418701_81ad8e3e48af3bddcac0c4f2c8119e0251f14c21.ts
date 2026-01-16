/// <reference path="person.ts"/>

enum RoomType {
	CommonRoom
}

class Work {
	progress: number;
	work: number;
	// result: ??;
	relatedStat: Stats;

	constructor(work: number) {
		this.progress = 0;
		this.work = work;
	}
}

class Room {
	type: RoomType
	people: Array<Person>;
	work: Work;

	constructor(type: RoomType) {
		this.people = new Array<Person>();
		this.type = type;
	}

	addPerson(person: Person): void {
		this.people.push(person);
		person.currentRoom = this;
	}

	removePerson(person: Person): void {
		let index = this.people.indexOf(person);
		if(index > -1) {
			this.people.splice(index, 1);
		}

		person.currentRoom = undefined;
	}

	movePerson(person: Person, target: Room): void {
		this.removePerson(person);
		target.addPerson(person);
	}
}