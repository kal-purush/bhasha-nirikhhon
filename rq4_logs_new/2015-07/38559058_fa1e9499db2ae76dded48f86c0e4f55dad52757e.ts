/// <reference path="../typings/jquery/jquery.d.ts" />
module Data {
    interface Query {
        q:string;
        page?: number;
        size: number;

    }
    interface Callback {
        success:(any)=>any;
        error:(any)=>any;
    }

    export function send(url:string, timeout:number, data:Query, callbacks:Callback) {
        $.ajax({
            url:url,
            method: 'GET',
            dataType: 'jsonp',
            timeout: timeout,
            data: data,
            success: function(data, textStatus, jqXHR) {
                callbacks.success(data);
            },
            error: function(jqXHR, textStatus, errorThrown) {
                callbacks.error(textStatus);
            }
        });
    }

    export function initModule() {

    }

}