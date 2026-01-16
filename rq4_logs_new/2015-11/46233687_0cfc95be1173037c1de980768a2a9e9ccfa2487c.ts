import {Http2ErrorType, Http2Error} from "./error";
import {HeaderField} from "./frame";

export class Compression {
    private static StaticTable = [
        {
            name: ":authority",
            value: ""
        },
        {
            name: ":method",
            value: "GET"
        },
        {
            name: ":method",
            value: "POST"
        },
        {
            name: ":path",
            value: "/"
        },
        {
            name: ":path",
            value: "/index.html"
        },
        {
            name: ":scheme",
            value: "http"
        },
        {
            name: ":scheme",
            value: "https"
        },
        {
            name: ":status",
            value: "200"
        },
        {
            name: ":status",
            value: "204"
        },
        {
            name: ":status",
            value: "206"
        },
        {
            name: ":status",
            value: "304"
        },
        {
            name: ":status",
            value: "400"
        },
        {
            name: ":status",
            value: "404"
        },
        {
            name: ":status",
            value: "500"
        },
        {
            name: "accept-charset",
            value: ""
        },
        {
            name: "accept-encoding",
            value: "gzip, deflate"
        },
        {
            name: "accept-language",
            value: ""
        },
        {
            name: "accept-ranges",
            value: ""
        },
        {
            name: "accept",
            value: ""
        },
        {
            name: "access-control-allow-origin",
            value: ""
        },
        {
            name: "age",
            value: ""
        },
        {
            name: "allow",
            value: ""
        },
        {
            name: "authorization",
            value: ""
        },
        {
            name: "cache-control",
            value: ""
        },
        {
            name: "content-disposition",
            value: ""
        },
        {
            name: "content-encoding",
            value: ""
        },
        {
            name: "content-language",
            value: ""
        },
        {
            name: "content-length",
            value: ""
        },
        {
            name: "content-location",
            value: ""
        },
        {
            name: "content-range",
            value: ""
        },
        {
            name: "content-type",
            value: ""
        },
        {
            name: "cookie",
            value: ""
        },
        {
            name: "date",
            value: ""
        },
        {
            name: "etag",
            value: ""
        },
        {
            name: "expect",
            value: ""
        },
        {
            name: "expires",
            value: ""
        },
        {
            name: "from",
            value: ""
        },
        {
            name: "host",
            value: ""
        },
        {
            name: "if-match",
            value: ""
        },
        {
            name: "if-modified-since",
            value: ""
        },
        {
            name: "if-none-match",
            value: ""
        },
        {
            name: "if-range",
            value: ""
        },
        {
            name: "if-unmodified-since",
            value: ""
        },
        {
            name: "last-modified",
            value: ""
        },
        {
            name: "link",
            value: ""
        },
        {
            name: "location",
            value: ""
        },
        {
            name: "max-forwards",
            value: ""
        },
        {
            name: "proxy-authenticate",
            value: ""
        },
        {
            name: "proxy-authorization",
            value: ""
        },
        {
            name: "range",
            value: ""
        },
        {
            name: "referer",
            value: ""
        },
        {
            name: "refresh",
            value: ""
        },
        {
            name: "retry-after",
            value: ""
        },
        {
            name: "server",
            value: ""
        },
        {
            name: "set-cookie",
            value: ""
        },
        {
            name: "strict-transport-security",
            value: ""
        },
        {
            name: "transfer-encoding",
            value: ""
        },
        {
            name: "user-agent",
            value: ""
        },
        {
            name: "vary",
            value: ""
        },
        {
            name: "via",
            value: ""
        },
        {
            name: "www-authenticate",
            value: ""
        }
    ];

    private _dynamicTable: HeaderField[];
    private _maxDynamicTableSize: number;

    constructor(maxDynamicTableSize: number) {
        this._dynamicTable = [];
        this._maxDynamicTableSize = maxDynamicTableSize;
    }

    private getSizeOfDynamicTable(): number {
        var length: number = 0;
        for (var item of this._dynamicTable) {
            length += Buffer.byteLength(item.name);
            length += Buffer.byteLength(item.value);
            length += 32;
        }
        return length;
    }

    private addHeaderFieldToDynamicTable(field: HeaderField): void {
        this._dynamicTable.splice(0, 0, field);
        while (this.getSizeOfDynamicTable() > this._maxDynamicTableSize) {
            this._dynamicTable.pop();
        }
    }

    private getHeaderFieldForIndex(index: number): HeaderField {
        if (index >= 1 && index <= Compression.StaticTable.length) {
            index = index - 1;
            return Compression.StaticTable[index];
        } else if (index >= Compression.StaticTable.length + 1 &&
            index <= Compression.StaticTable.length +
            this._dynamicTable.length) {
            index = index - Compression.StaticTable.length - 1;
            return this._dynamicTable[index];
        } else {
            throw new Http2Error("Invalid compression index",
                Http2ErrorType.CompressionError)
        }
    }
}