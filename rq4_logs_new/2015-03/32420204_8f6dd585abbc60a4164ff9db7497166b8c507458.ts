//baseevent.ts
import InfoData = require('../../infodata');
//
import BaseItem = require('./baseitem');
import DescriptionItem = require('./descriptionitem');
//
class BaseEvent extends DescriptionItem implements InfoData.IBaseItem {
  //
  private _departementid: any;
  private _anneeid: any;
  private _semestreid: any;
  private _groupeid: any;
  private _personid: any;
  private _date: Date;
  private _uniteid: any;
  private _matiereid: any;
  private _genre: string;
  private _docs: any[];
  private _status: string;
  //
  constructor(oMap?: any) {
    super(oMap);
    if ((oMap !== undefined) && (oMap !== null)) {
      if (oMap.departementid !== undefined) {
        this.departementid = oMap.departementid;
      }
      if (oMap.anneeid !== undefined) {
        this.anneeid = oMap.anneeid;
      }
      if (oMap.semestreid !== undefined) {
        this.semestreid = oMap.semestreid;
      }
      if (oMap.groupeid !== undefined) {
        this.groupeid = oMap.groupeid;
      }
      if (oMap.personid !== undefined) {
        this.personid = oMap.personid;
      }
      if (oMap.uniteid !== undefined) {
        this.uniteid = oMap.uniteid;
      }
      if (oMap.matiereid !== undefined) {
        this.matiereid = oMap.matiereid;
      }
      if (oMap.genre !== undefined) {
        this.genre = oMap.genre;
      }
      if (oMap.status !== undefined) {
        this.status = oMap.status;
      }
      if (oMap.documentids !== undefined) {
        this.documentids = oMap.documentids;
      }
      if (oMap.date !== undefined) {
        this.date = oMap.date;
      }

    }// oMap
  }// constructor
  //
  public get documentids(): any[] {
    return (this._docs !== undefined) ? this._docs : null;
  }
  public set documentids(s: any[]) {
    if ((s !== undefined) && (s !== null) && (s.length > 0)) {
      this._docs = s;
    } else {
      this._docs = null;
    }
  }
  public get has_documentids(): boolean {
    return ((this.documentids !== null) && (this.documentids.length > 0));
  }
  public add_documentid(id: any): void {
    if ((id !== undefined) && (id !== null) &&
      (id.toString().trim().length > 0)) {
      if ((this._docs === undefined) || (this._docs === null)) {
        this._docs = [];
      }
      BaseItem.array_add(this._docs, id);
    }
  }
  public remove_documentid(id: any): void {
    if ((id !== undefined) && (id !== null) &&
      (id.toString().trim().length > 0)) {
      if ((this._docs !== undefined) && (this._docs !== null) &&
        (this._docs.length > 0)) {
        BaseItem.array_remove(this._docs, id);
      }
    }
  }
  //
  public get status(): string {
    return (this._status !== undefined) ? this._status : null;
  }
  public set status(s: string) {
    if ((s !== undefined) && (s !== null) && (s.trim().length > 0)) {
      this._status = s.trim().toLowerCase();
    } else {
      this._status = null;
    }
  }
  public get has_status(): boolean {
    return (this.status !== null);
  }
  //
  public get genre(): string {
    return (this._genre !== undefined) ? this._genre : null;
  }
  public set genre(s: string) {
    if ((s !== undefined) && (s !== null) && (s.trim().length > 0)) {
      this._genre = s.trim().toLowerCase();
    } else {
      this._genre = null;
    }
  }
  public get has_genre(): boolean {
    return (this.genre !== null);
  }
  //
  public get date(): Date {
    return (this._date !== undefined) ? this._date : null;
  }
  public set date(d: Date) {
    this._date = BaseItem.check_date(d);
  }
  public get has_date(): boolean {
    return (this.date !== null);
  }
  //
  public get matiereid(): any {
    return (this._matiereid !== undefined) ? this._matiereid : null;
  }
  public set matiereid(s: any) {
    if ((s !== undefined) && (s !== null) && (s.toString().trim().length > 0)) {
      this._matiereid = s;
    } else {
      this._matiereid = null;
    }
  }
  public get has_matiereid(): boolean {
    return (this.uniteid !== null);
  }
  //
  public get uniteid(): any {
    return (this._uniteid !== undefined) ? this._uniteid : null;
  }
  public set uniteid(s: any) {
    if ((s !== undefined) && (s !== null) && (s.toString().trim().length > 0)) {
      this._uniteid = s;
    } else {
      this._uniteid = null;
    }
  }
  public get has_uniteid(): boolean {
    return (this.uniteid !== null);
  }
  //
  public get personid(): any {
    return (this._personid !== undefined) ? this._personid : null;
  }
  public set personid(s: any) {
    if ((s !== undefined) && (s !== null) && (s.toString().trim().length > 0)) {
      this._personid = s;
    } else {
      this._personid = null;
    }
  }
  public get has_personid(): boolean {
    return (this.personid !== null);
  }
  //
  public get groupeid(): any {
    return (this._groupeid !== undefined) ? this._groupeid : null;
  }
  public set groupeid(s: any) {
    if ((s !== undefined) && (s !== null) && (s.toString().trim().length > 0)) {
      this._groupeid = s;
    } else {
      this._groupeid = null;
    }
  }
  public get has_groupeid(): boolean {
    return (this.groupeid !== null);
  }
  //
  public get anneeid(): any {
    return (this._anneeid !== undefined) ? this._anneeid : null;
  }
  public set anneeid(s: any) {
    if ((s !== undefined) && (s !== null) && (s.toString().trim().length > 0)) {
      this._anneeid = s;
    } else {
      this._anneeid = null;
    }
  }
  public get has_anneeid(): boolean {
    return (this.anneeid !== null);
  }
  //
  public get semestreid(): any {
    return (this._semestreid !== undefined) ? this._semestreid : null;
  }
  public set semestreid(s: any) {
    if ((s !== undefined) && (s !== null) && (s.toString().trim().length > 0)) {
      this._semestreid = s;
    } else {
      this._semestreid = null;
    }
  }
  public get has_semestreid(): boolean {
    return (this.semestreid !== null);
  }
  //
  public get departementid(): any {
    return (this._departementid !== undefined) ? this._departementid : null;
  }
  public set departementid(s: any) {
    if ((s !== undefined) && (s !== null) && (s.toString().trim().length > 0)) {
      this._departementid = s;
    } else {
      this._departementid = null;
    }
  }
  public get has_departementid(): boolean {
    return (this.departementid !== null);
  }
  //
  public get is_storeable(): boolean {
    return (this.type !== null) && (this.collection_name !== null) &&
      this.has_departementid && this.has_anneeid && this.has_semestreid &&
      this.has_uniteid && this.has_matiereid && this.has_groupeid &&
      this.has_genre && this.has_date;
  }
  public to_insert_map(oMap: any): void {
    super.to_insert_map(oMap);
    if (this.has_departementid) {
      oMap.departementid = this.departementid;
    }
    if (this.has_anneeid) {
      oMap.anneeid = this.anneeid;
    }
    if (this.has_semestreid) {
      oMap.semestreid = this.semestreid;
    }
    if (this.has_groupeid) {
      oMap.groupeid = this.groupeid;
    }
    if (this.has_personid) {
      oMap.personid = this.personid;
    }
    if (this.has_date) {
      oMap.date = this.date;
    }
    if (this.has_genre) {
      oMap.genre = this.genre;
    }
    if (this.has_uniteid) {
      oMap.uniteid = this.uniteid;
    }
    if (this.has_matiereid) {
      oMap.matiereid = this.matiereid;
    }
    if (this.has_status){
      oMap.status = this.status;
    }
    if (this.has_documentids){
      oMap.documentids = this.documentids;
    }
  }// to_insert_map
  public to_fetch_map(oMap: any) : void {
    super.to_fetch_map(oMap);
    if (oMap.documentids !== undefined){
      oMap.documentids = null;
    }

  }
}// class Affectation
export = BaseEvent;