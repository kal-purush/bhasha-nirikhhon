export class Multimedia {
    
    public event_title: string;
    public event_date: string;
    public picture_key: string;
    public picture_name: string;
    public owner : string;
    public tags : string[];

    

    constructor(picture_key: string, event_title: string, event_date:string,tags: string[], picture_name:string, owner: string){
        this.picture_key = picture_key,
        this.event_title = event_title,
        this.event_date = event_date,
        this.tags = tags
        this.picture_name = picture_name
        this.owner = owner
    }
    
}