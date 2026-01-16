import { IGroup, IUserWithPosition } from '../models/type';

export class Group implements IGroup {
	name: string;
	code: string;
	date_create?: string;
	date_modify?: string;
	id?: string;
	count?: number;
	user?: IUserWithPosition[];
	boss?: IUserWithPosition[];
	constructor(
		data: IGroup = <IGroup>{}
	) {
		this.id = data.id || undefined;
		this.name = data.name || '';
		this.code = data.code || '';
		this.date_create = data.date_create || '';
		this.date_modify = data.date_modify || '';
		this.user = data.user || [];
		if(data.boss){
			this.boss = data.boss.map( item => {
				if(!item.header_type || !item.header_type.name) item.header_type = {name: ''};
				return item;
			});
		} else {
			this.boss = [];
		}
	}
}