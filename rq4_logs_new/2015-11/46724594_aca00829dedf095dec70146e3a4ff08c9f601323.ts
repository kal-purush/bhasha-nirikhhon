//attacheddocs.ts
import {AttachedDocModel} from '../data/attacheddocmodel';
import {InfoUserInfo} from '../common/infouserinfo';
//
export class AttachedDocs extends AttachedDocModel {
	//
	static inject() { return [InfoUserInfo] }
	//
	constructor(info: InfoUserInfo) {
		super(info);
	}
}// AttachedDocs
