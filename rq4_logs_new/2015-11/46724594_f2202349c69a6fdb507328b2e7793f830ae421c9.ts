//basedetailmodel.ts
//
import {BaseView} from './baseview';
import {UserInfo} from './userinfo';
import {IInfoEvent} from 'infodata';
//
export class BaseDetailModel<T extends IInfoEvent> extends BaseView {
    //
	private _evt: T;
	private _start: string;
	private _end: string;
    //
    constructor(userinfo: UserInfo) {
        super(userinfo);
    }
	protected initialize_event(evtid: string): Promise<boolean> {
		this.clear_error();
		this._evt = null;
		return this.dataService.find_item_by_id(evtid).then((p: T) => {
			this.event = p;
			return this.retrieve_one_avatar(this.event);
		}).then((x) => {
			return true;
		});
	}// initialize_groupeevent
	public get canSave():boolean {
		return (this.event !== null) && this.event.is_storeable();
	}
	public get cannotSave():boolean {
		return (!this.canSave);
	}
	public save(): Promise<any> {
		let p = this.event;
		if (p === null){
			return Promise.resolve(false);
		}
		if (!p.is_storeable()) {
			return Promise.resolve(false);
		}
		this.clear_error();
		return this.dataService.save_item(p).then((b)=>{
			if ((b !== undefined) && (b !== null) && (b == true)){
				this.info_message = "Item modofié!";
			} else {
				this.error_message = "Erreur enregistrement...";
			}
		}).catch((e)=>{
			this.set_error(e);
		})
	}// save
	public deactivate(): any {
		if ((this.event !== null) && (this.event.url !== null)) {
			this.revokeUrl(this.event.url);
			this.event.url = null;
		}
	}
	public get event(): T {
		return (this._evt !== undefined) ? this._evt : null;
	}
	public set event(s: T) {
		this._evt = (s !== undefined) ? s : null;
		this._start = null;
		this._end = null;
		if (this._evt !== null) {
			let d = this._evt.semestreMinDate;
			if ((d !== undefined) && (d !== null)) {
				this._start = d.toISOString().substr(0, 10);;
			}
			d = this._evt.semestreMaxDate;
			if ((d !== undefined) && (d !== null)) {
				this._end = d.toISOString().substr(0, 10);;
			}
		}// evt
	}
	public get eventUrl(): string {
		return (this.event !== null) ? this.event.url : null;
	}
	public get hasEventUrl(): boolean {
		return (this.eventUrl !== null);
	}// hasUrl
	public get genre(): string {
		return (this.event !== null) ? this.event.genre : null;
	}
	public set genre(s: string) {
		if (this.event !== null){
			this.event.genre = s;
		}
	}
	public get description(): string {
		return (this.event !== null) ? this.event.description : null;
	}
	public set description(s: string) {
		if (this.event !== null){
			this.event.description = s;
		}
	 }
	 public get status(): string {
		return (this.event !== null) ? this.event.status : null;
	}
	public set status(s: string) {
		if (this.event !== null){
			this.event.status = s;
		}
	}
	public get eventDate(): string {
		let d:Date = null;
		if (this.event !== null){
			d = this.event.eventDate;
		}
		return this.date_to_string(d);
	}
	public set eventDate(s: string) {
		if (this.event !== null){
			this.event.eventDate = this.string_to_date(s);
		}
	}
	public get minDate(): string {
		return (this._start !== undefined) ? this._start : null;
	}
	public set minDate(s: string) {
		this._start = this.check_string(s);
	}
	public get maxDate(): string {
		return (this._end !== undefined) ? this._end : null;
	}
	public set endDate(s: string) {
		this._end = this.check_string(s);
	}
	public get semestreName(): string {
		return (this.event !== null) ? this.event.semestreName : null;
	}
	public get departementName(): string {
		return (this.event !== null) ? this.event.departementName : null;
	}
	public get anneeName(): string {
		return (this.event !== null) ? this.event.anneeName : null;
	}
	public get groupeName(): string {
		return (this.event !== null) ? this.event.groupeName : null;
	}
	public get uniteName(): string {
		return (this.event !== null) ? this.event.uniteName : null;
	}
	public get matiereName(): string {
		return (this.event !== null) ? this.event.matiereName : null;
	}
}