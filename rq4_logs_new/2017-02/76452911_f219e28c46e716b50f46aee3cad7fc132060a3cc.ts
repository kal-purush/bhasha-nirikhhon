import * as PythonShell from "python-shell";


export interface Pdu {
        pdu: string;
        length: number; //length to pass to AT+CMGS
        seq: number; //part number over total
        cnt: number; //total number of parts
}

export type byte = number;

export interface Sms {
        text: string;
        pid: byte; //Protocol identifier
        dcs: byte; //data coding scheme
        csca: string; //Service center address
        number: "SR-OK" | "SR-UNKNOWN" | "SR-STORED" | string;
        type: TP_MTI; //2bit, pdu type
        date: Date;
        fmt: Fmt; //Encoding format
        //if SMS-STATUS-REPORT
        sr?: {
                recipient: string; //phone number
                scts: Date; //service center time tamp
                dt: Date; //discharge time
                status: TP_ST | byte;
        };
        //User Data Header present => SMS-DELIVER multipart or SMS-STATUS-REPORT
        ref?: number; //message ref returned by CMGS
        cnt?: number; //total
        seq?: number; //number over total
}


export class SmsBase {

        public readonly _fmt: string;
        public readonly _type: string;

        constructor(
                public readonly type: TP_MTI,
                public readonly text: string,
                public readonly pid: byte,
                public readonly dcs: byte,
                public readonly csca: string,
                public readonly number: "SR-OK" | "SR-UNKNOWN" | "SR-STORED" | string,
                public readonly date: Date,
                public readonly fmt: Fmt
        ) {
                this._fmt= Fmt[this.fmt];
                this._type= TP_MTI[this.type];
        }

}

export class SmsWithDataHeader extends SmsBase implements Sms {

        constructor(
                type: typeof SmsBase.prototype.type,
                text: typeof SmsBase.prototype.text,
                pid: typeof SmsBase.prototype.pid,
                dcs: typeof SmsBase.prototype.dcs,
                csca: typeof SmsBase.prototype.csca,
                number: typeof SmsBase.prototype.number,
                date: typeof SmsBase.prototype.date,
                fmt: typeof SmsBase.prototype.fmt,
                public readonly ref: number,
                public readonly cnt: number,
                public readonly seq: number
        ){
                super( type, text, pid, dcs, csca, number, date, fmt);
        }

}

export class SmsDeliverPart extends SmsWithDataHeader implements Sms{

        constructor(
                text: typeof SmsBase.prototype.text,
                pid: typeof SmsBase.prototype.pid,
                dcs: typeof SmsBase.prototype.dcs,
                csca: typeof SmsBase.prototype.csca,
                number: typeof SmsBase.prototype.number,
                date: typeof SmsBase.prototype.date,
                fmt: typeof SmsBase.prototype.fmt,
                ref: typeof SmsWithDataHeader.prototype.ref,
                cnt: typeof SmsWithDataHeader.prototype.cnt,
                seq: typeof SmsWithDataHeader.prototype.seq
        ){
                super(TP_MTI.SMS_DELIVER, text, pid, dcs, csca, number, date, fmt, ref, cnt, seq);
        }
}

export class SmsDeliver extends SmsBase implements Sms{

        constructor(
                text: typeof SmsBase.prototype.text,
                pid: typeof SmsBase.prototype.pid,
                dcs: typeof SmsBase.prototype.dcs,
                csca: typeof SmsBase.prototype.csca,
                number: typeof SmsBase.prototype.number,
                date: typeof SmsBase.prototype.date,
                fmt: typeof SmsBase.prototype.fmt
        ){
                super(TP_MTI.SMS_DELIVER, text, pid, dcs, csca, number, date, fmt);
        }

}

export class SmsStatusReport extends SmsWithDataHeader implements Sms{

        public readonly _status: string;
        public readonly _stClass: ST_CLASS;

        constructor(
                text: typeof SmsBase.prototype.text,
                pid: typeof SmsBase.prototype.pid,
                dcs: typeof SmsBase.prototype.dcs,
                csca: typeof SmsBase.prototype.csca,
                number: typeof SmsBase.prototype.number,
                date: typeof SmsBase.prototype.date,
                fmt: typeof SmsBase.prototype.fmt,
                ref: typeof SmsWithDataHeader.prototype.ref,
                cnt: typeof SmsWithDataHeader.prototype.cnt,
                seq: typeof SmsWithDataHeader.prototype.seq,
                public readonly sr: {
                        recipient: string;
                        scts: Date;
                        dt: Date;
                        status: TP_ST | byte;
                }
        ) {
                super(TP_MTI.SMS_STATUS_REPORT, text, pid, dcs, csca, number, date, fmt, ref, cnt, seq);

                this._status= (TP_ST[this.sr.status]!==undefined)?TP_ST[this.sr.status]:"RESERVED";
                this._stClass= stClassOf(this.sr.status);

        }
}


export enum Fmt {
        GSM0338 = 0,
        EIGHT_BIT = 4,
        UCS2 = 8
}

export enum TP_MTI {
        SMS_DELIVER = 0b00,
        SMS_STATUS_REPORT = 0b10,
        SMS_SUBMIT_REPORT = 0b01 //Not supported currently
}

//03.40.pdf p57
export enum TP_ST {
        COMPLETED_RECEIVED = 0b00000000,
        COMPLETED_UNABLE_TO_CONFIRM_DELIVERY = 0b00000001,
        COMPLETED_REPLACED = 0b00000010,

        STILL_TRYING_CONGESTION = 0b00100000,
        STILL_TRYING_SME_BUSY = 0b00100001,
        STILL_TRYING_NO_RESPONSE_FROM_SME = 0b00100010,
        STILL_TRYING_SERVICE_REJECTED = 0b00100011,
        STILL_TRYING_QOS_UNAVAILABLE = 0b00100100,
        STILL_TRYING_SME_ERROR = 0b00100101,

        PERMANENT_ERROR_REMOTE_PROCEDURE_ERROR = 0b01000000,
        PERMANENT_ERROR_INCOMPATIBLE_DESTINATION = 0b01000001,
        PERMANENT_ERROR_CONNECTION_REJECTED_BY_SME = 0b01000010,
        PERMANENT_ERROR_NOT_OBTAINABLE = 0b01000011,
        PERMANENT_ERROR_QOS_UNAVAILABLE = 0b01000100,
        PERMANENT_ERROR_NO_INTERWORKING_AVAILABLE = 0b01000101,
        PERMANENT_ERROR_SM_VALIDITY_PERIOD_EXPIRED = 0b01000110,
        PERMANENT_ERROR_SM_DELETED_BY_ORIGINATING_SME = 0b01000111,
        PERMANENT_ERROR_SM_DELETED_BY_SC_ADMINISTRATION = 0b01001000,
        PERMANENT_ERROR_SM_DOES_NOT_EXIST = 0b01001001,

        TEMPORARY_ERROR_CONGESTION = 0b01100000,
        TEMPORARY_ERROR_SME_BUSY = 0b01100001,
        TEMPORARY_ERROR_NO_RESPONSE_FROM_SME = 0b01100010,
        TEMPORARY_ERROR_SERVICE_REJECTED = 0b01100011,
        TEMPORARY_ERROR_QOS_UNAVAILABLE = 0b01100100,
        TEMPORARY_ERROR_SME_ERROR = 0b01100101
}

export type ST_CLASS = "COMPLETED" | "STILL TRYING" | "PERMANENT ERROR" | "TEMPORARY ERROR" | "RESERVED" | "SPECIFIC TO SC";

//All reserved should be interpreted as STILL_TRYING_SERVICE_REJECTED
function stClassOf(st: TP_ST): ST_CLASS {

        if (0b00000000 <= st && st <= 0b00000010) return "COMPLETED";
        if (0b00100000 <= st && st <= 0b00100101) return "STILL TRYING";
        if (0b01000000 <= st && st <= 0b01001001) return "PERMANENT ERROR";
        if (0b01100000 <= st && st <= 0b01100101) return "TEMPORARY ERROR";

        if (
                (0b10000000 <= st && st <= 0b11111111) ||
                (0b00000011 <= st && st <= 0b00001111) ||
                (0b00100110 <= st && st <= 0b00101111) ||
                (0b01001010 <= st && st <= 0b01001111) ||
                (0b01100110 <= st && st <= 0b01101001) ||
                (0b01101010 <= st && st <= 0b01101111)
        ) return "RESERVED";

        return "SPECIFIC TO SC";

}

function descriptorToInstance(descriptor: Sms): SmsDeliver | SmsDeliverPart | SmsStatusReport {

        switch (descriptor.type) {
                case TP_MTI.SMS_STATUS_REPORT:
                        return new SmsStatusReport(
                                        descriptor.text,
                                        descriptor.pid,
                                        descriptor.dcs,
                                        descriptor.csca,
                                        descriptor.number,
                                        descriptor.date,
                                        descriptor.fmt,
                                        descriptor.ref as any,
                                        descriptor.cnt as any,
                                        descriptor.seq as any,
                                        descriptor.sr as any
                        );
                case TP_MTI.SMS_DELIVER:
                        if ( descriptor.hasOwnProperty("ref") )
                                return new SmsDeliverPart(
                                        descriptor.text,
                                        descriptor.pid,
                                        descriptor.dcs,
                                        descriptor.csca,
                                        descriptor.number,
                                        descriptor.date,
                                        descriptor.fmt,
                                        descriptor.ref as any,
                                        descriptor.cnt as any,
                                        descriptor.seq as any
                                );
                        else
                                return new SmsDeliver(
                                        descriptor.text,
                                        descriptor.pid,
                                        descriptor.dcs,
                                        descriptor.csca,
                                        descriptor.number,
                                        descriptor.date,
                                        descriptor.fmt
                                );
                case TP_MTI.SMS_SUBMIT_REPORT:
                        throw new Error("SUBMIT REPORT not supported");
        }


}


//DECODE service center ( SC ) to MS ( mobile station switched on with SIM module )
export function decodePdu(pdu: string, callback: (error: null | Error, sms: Sms) => void): void {

        bridge("smsDeliver", { "pdu": pdu }, function (error, out) {

                if (error) return callback(error, {} as Sms);

                if (out.date) out.date = new Date(out.date);

                if (out.type === 0b10) {
                        if (out.sr.dt) out.sr.dt = new Date(out.sr.dt);
                        if (out.sr.scts) out.sr.scts = new Date(out.sr.scts);
                }

                callback(null, descriptorToInstance(out));

        });

}

export function buildSmsSubmitPdus(params: {
        number: string;
        text: string;
        validity?: Date;
        csca?: string;
        klass?: number;
        request_status?: boolean;
}, callback: (error: Error, pdus: Pdu[]) => void): void {

        let args: any = {};

        Object.assign(args, params);

        if (params.validity instanceof Date) args.validity = params.validity.toUTCString();

        bridge("smsSubmit", args, callback);

}


let options = {
        "mode": "text",
        "pythonPath": __dirname + "/../../out/virtual/bin/python",
        "pythonOptions": ['-u'],
        "scriptPath": __dirname + "/../../src/lib/"
};

function bridge(method: string, args: any, callback: (error: null | Error, out: any) => void): void {

        PythonShell.run("bridge.py", Object.assign({
                "args": [method, JSON.stringify(args)]
        }, options), function (error, out) {

                if (error) return callback(error, null);

                callback(null, JSON.parse(out[0]));

        });

}