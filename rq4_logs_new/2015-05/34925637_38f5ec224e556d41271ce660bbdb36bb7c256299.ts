//affectation.ts
//
import {IAffectation, IPerson} from '../../infodata.d';
import {WorkItem} from './workitem';
import {InfoRoot} from '../../inforoot';
//
export class Affectation extends WorkItem
    implements IAffectation {
    private _start: Date;
    private _end: Date;
    //
    constructor(oMap?: any) {
        super(oMap);
        if ((oMap !== undefined) && (oMap !== null)) {
            if (oMap.startDate !== undefined) {
                this.startDate = oMap.startDate;
            }
            if (oMap.endDate !== undefined) {
                this.endDate = oMap.endDate;
            }
        } // oMap
    } // constructor
    public get startDate(): Date {
        return (this._start !== undefined) ? this._start : null;
    }
    public set startDate(d: Date) {
        this._start = InfoRoot.check_date(d);
    }
    public get endDate(): Date {
        return (this._end !== undefined) ? this._end : null;
    }
    public set endDate(d: Date) {
        this._end = InfoRoot.check_date(d);
    }
    public to_map(oMap: any): void {
        super.to_map(oMap);
        if (this.startDate !== null) {
            oMap.startDate = this.startDate;
        }
        if (this.endDate !== null) {
            oMap.endDate = this.endDate;
        }
    } // toInsertMap
    public update_person(pPers: IPerson): void {
        if ((pPers !== undefined) && (pPers !== null)) {
            super.update_person(pPers);
            let cont: string[] = pPers.affectationids;
            if (cont === null) {
                cont = [];
            }
            InfoRoot.add_id_to_array(cont, this.id);
            pPers.affectationids = cont;
        }// pPers
    }// update_person
    public is_storeable(): boolean {
        if (!super.is_storeable()) {
            return false;
        }
        if ((this.startDate === null) || (this.endDate === null)) {
            return true;
        }
        let t1 = Date.parse(this.startDate.toString());
        let t2 = Date.parse(this.endDate.toString());
        if (isNaN(t1) || isNaN(t2)) {
            return false;
        }
        return (t1 <= t2);
    }// is_storeable
}