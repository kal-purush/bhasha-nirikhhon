export class CalendarUser{
    constructor(id='',title='',start='',end='',owner=''){
            this.id=id;
            this.title=title;
            this.start=start;
            this.end=end;
            this.owner=owner;
    }
        id: string;
        title: string;
        start: string;
        end: string;
        owner:string;
    }