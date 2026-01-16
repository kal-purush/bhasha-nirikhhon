//baseitemdetail.ts
//
import {BaseView} from './baseview';
import {UserInfo} from './userinfo';
import {IPersonItem} from 'infodata';
//
export class BaseDetailModel<T extends IPersonItem> extends BaseView {
    //
	private _item: T;
	private _canEdit:boolean;
    //
    constructor(userinfo: UserInfo) {
        super(userinfo);
    }
	public get currentItem(): T {
		return (this._item !== undefined) ? this._item : null;
	}
	protected post_set_current_item(): Promise<any> {
		return Promise.resolve(true);
	}
	public set currentItem(s: T) {
		this._item = (s !== undefined) ? s : null;
		this.post_set_current_item();
	}
	public get canEdit():boolean {
		if ((this._canEdit === undefined)|| (this._canEdit === null)){
			this._canEdit = false;
		}
		return this._canEdit;
	}
	public set canEdit(s:boolean){
		this._canEdit = s;
	}
	public get isReadOnly():boolean {
		return (!this.canEdit);
	}
	protected initialize_item(evtid: string): Promise<boolean> {
		this.clear_error();
		this._item = null;
		return this.dataService.find_item_by_id(evtid).then((p: T) => {
			this.currentItem = p;
			return this.retrieve_one_avatar(this.currentItem);
		}).then((x) => {
			return true;
		});
	}// initialize_groupeevent
	public get canSave():boolean {
		return (this.currentItem !== null) && this.currentItem.is_storeable();
	}
	public get cannotSave():boolean {
		return (!this.canSave);
	}
	public save(): Promise<any> {
		let p = this.currentItem;
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
	public activate(params?: any, config?: any, instruction?: any): any {
		let id: string = null;
		if (params.id !== undefined) {
			id = params.id;
		}
		let p = this.currentItem;
		if (p !== null) {
			if (p.url !== null) {
				this.revokeUrl(p.url);
			}
		}
		this.canEdit = false;
		this.currentItem = null;
		return this.initialize_item(id);
	}// activate
	public deactivate(): any {
		if ((this.currentItem !== null) && (this.currentItem.url !== null)) {
			this.revokeUrl(this.currentItem.url);
			this.currentItem.url = null;
		}
	}
	public get itemUrl(): string {
		return (this.currentItem !== null) ? this.currentItem.url : null;
	}
	public get hasItemUrl(): boolean {
		return (this.itemUrl !== null);
	}// hasUrl
	public get description(): string {
		return (this.currentItem !== null) ? this.currentItem.description : null;
	}
	public set description(s: string) {
		if (this.currentItem !== null){
			this.currentItem.description = s;
		}
	 }
	 public get status(): string {
		return (this.currentItem !== null) ? this.currentItem.status : null;
	}
	public set status(s: string) {
		if (this.currentItem !== null){
			this.currentItem.status = s;
		}
	}
	public get departementName(): string {
		return (this.currentItem !== null) ? this.currentItem.departementName : null;
	}
}