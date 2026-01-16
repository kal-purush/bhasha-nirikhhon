//administratorsmodel.ts
//
import {UserInfo} from './userinfo';
import {PersonViewModel} from './personmodel';
import {IAdministrator} from 'infodata';
//
export class AdministratorsModel extends PersonViewModel<IAdministrator> {
	//
    constructor(info: UserInfo) {
        super(info);
        this.title = 'Opérateurs';
    }// constructor
    protected create_item(): IAdministrator {
        return this.itemFactory.create_administrator({
			departementid:this.departementid,
			departementName: this.departementName
		});
    }
}// class AdministratorsModel