/// <reference path="../Markdown.ts"/>
var $;
module github {
    export class Client implements markdown.Client {
        toHTML(source : string, callback : markdown.Callback) {
            var params = {
                text : source,
                mode : 'markdown',
            };
            $.ajax({
                url : 'https://api.github.com/markdown',
		type : 'POST',
		dataType : 'html',
		scriptCharset: 'utf-8',
		processData : false,
                data : JSON.stringify(params),
            }).done((data_ : any, status : string, data : any) => {
                var headers = data.getAllResponseHeaders();
                callback.success(data.responseText,
                                 Number(data.getResponseHeader('X-RateLimit-Remaining')));
	    }).fail((data : any) => {
		callback.error(data.status, data.responseText);
	    });
        }
    }
}