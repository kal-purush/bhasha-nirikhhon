export class Article {
	title: string;
	link: string;
	priority: number;

	constructor(title, link, priority) {
		this.title = title;
		this.link = link;
		this.priority = priority;
	}

	priorityUp() {
		this.priority++;
		console.log(this.priority);
		return false;
	}

	priorityDown() {
		this.priority--;
		console.log(this.priority);
		return false;
	}
}