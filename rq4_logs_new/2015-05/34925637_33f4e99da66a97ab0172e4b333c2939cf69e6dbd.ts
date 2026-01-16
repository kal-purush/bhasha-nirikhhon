//administrator.ts
//
import {IPerson, IAdministrator} from '../../infodata.d';
import {DepartementPerson} from './depperson';
import {InfoRoot} from '../../inforoot';
import {ADMINISTRATOR_TYPE, ADMINISTRATOR_PREFIX, ROLE_ADMIN} from '../../infoconstants';
//
export class Administrator extends DepartementPerson implements IAdministrator {
    //
    constructor(oMap?: any) {
        super(oMap);
    }// constructor
    public type(): string {
        return ADMINISTRATOR_TYPE;
    }
    public base_prefix(): string {
        return ADMINISTRATOR_PREFIX;
    }
    public update_person(pPers: IPerson): void {
        if ((pPers !== undefined) && (pPers !== null)) {
            super.update_person(pPers);
            if (!pPers.is_admin) {
                let x: string[] = pPers.roles;
                if (x === null) {
                    x = [];
                }
                x.push(ROLE_ADMIN);
                pPers.roles = x;
            }
        }// pPers
    }// update_person
}// class Enseignant