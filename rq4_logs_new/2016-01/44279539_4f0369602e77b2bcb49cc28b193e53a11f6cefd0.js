var first  = ['student','data visualization designer','graphic designer','writter','photographer','video editor'];
var i = 0;
        
var maxfirst  = first.length - 1;
function delay() {
    $('#name').velocity("transi1ion.slideUpIn", 100000);
        setInterval(firstwordchange, 3000);
}
function firstwordchange() {
    if (i < maxfirst) i++; else i = 0;

    $('#replace').velocity("transition.slideUpOut", 500);

    setTimeout(function () {
        $('#replace').text(first[i]);
    }, 400);

    $('#replace').velocity("transition.slideUpIn", 500);
}
        
setTimeout(delay, 600);