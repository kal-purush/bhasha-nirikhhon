export class Episode {
    e_id: number;
    description: string;
    length: string;
    title: string;
    date_published: string;
    creator: number;
    podcast: number;

    constructor(e_id, description, length, title, date_published, creator, podcast) {
        this.e_id = e_id;
        this.description = description;
        this.length = length;
        this.title = title;
        this.date_published = date_published;
        this.creator = creator;
        this.podcast = podcast;
    }
}