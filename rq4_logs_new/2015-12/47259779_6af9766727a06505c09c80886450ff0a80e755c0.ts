/**
 * Created by MIC on 2015/12/1.
 */

import {TbPageListItem} from "./contracts/TbPageList";
import {TbProjectDetailResponse} from "./contracts/TbProjectDetail";

export interface DbMessage {
    code:number;
    completed:boolean;
    error?:any;
}

export interface DbStartMessage extends DbMessage {
    ids:string[];
    totals?:number[];
}

export interface DbPageMessage extends DbMessage {
    ids:string[];
    totals?:number[];
    items?:TbPageListItem[];
}

export interface DbProjectMessage extends DbMessage {
    projectID:string;
    response?:TbProjectDetailResponse;
}

export abstract class DbMessageCode {

    static get NONE():number {
        return 0;
    }

    static get START():number {
        return 1;
    }

    static get LIST_TYPE_PAGE():number {
        return 2;
    }

    static get GET_PROJECT_DETAIL():number {
        return 3;
    }

}