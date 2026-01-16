/**
 * Chrome Extension - Adds an Open button to Swagger UI, for easier reading of exeception pages (like .NET uses). 
 */
var code = $('.highlight-code');
MutationObserver = window.MutationObserver || window.WebKitMutationObserver;

var observer = new MutationObserver(function (mutations, observer) {
    code = $('.live-responses-table .highlight-code');
    $.each(code, (i, val) => {
        let hasButton = $(val).find('.swagger_open').length > 0;

        if (!hasButton) {
            $(val).append(`
            <div class="swagger_open download-contents" style="right:100px">Open</div>
            `);
        }
    });
});

observer.observe(document, {
    subtree: true,
    attributes: true
});

$(document).on('click', '.swagger_open', function (e) {
    let parent = e.target.parentElement;
    //gets html from "pre" element
    let code = $(parent).find('.microlight')[0];
    let html = $(code).text();

    //opens the new tab and writes the html to it.
    var w = window.open();
    w.document.write(html);
    w.document.close();
});