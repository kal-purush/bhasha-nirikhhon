

export class SearcherUser{
    user_name: string;
    last_name: string;
    artistic_name: string;
    location: string;
    tag: string;
    discipline_name:string;

    constructor(item?: any) {
        this.user_name = item?.user_name || '';
        this.last_name = item?.last_name || '';
        this.artistic_name = item?.artistic_name || ''; ;
        this.location = item?.location || '';
        this.tag = item?.tag || '';
        this.discipline_name = item?.discipline_name || '';
    }
}