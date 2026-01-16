
/**
 * 页面加后执行 fixedTableHeader
 * @returns {*|jQuery|HTMLElement}
 */
$.fn.fixedTableHeader = function () {
    var table = $(this);
    var tClass = table.attr('class'),
        tWidth = table.width();
    var tableOffset = table.offset().top;
    var fixedTable = $('<table id="fixedTable"></table>').css({ 'display':'none', 'position':'fixed', 'top':'0px','width':tWidth});
    fixedTable.attr('class',tClass);
    table.parent().append(fixedTable.append(table.find("thead").clone()));


    fixedTable.find("th").each(function (i) {
        var tThWidth = table.find("th").eq(i).width();
        $(this).width(tThWidth);
    });
    
    $(window).off('scroll.fixed').on('scroll.fixed', function () {
        var offset = $(this).scrollTop();
        if (offset >= tableOffset && fixedTable.is(":hidden")) {
            fixedTable.show();
        }
        else if (offset < tableOffset) {
            fixedTable.hide();
        }
    });
    return table;
};
/**
 * 表格重新构建（eg，ajax重刷表格） 调用 fixedTableHeaderResize
 * @returns {*|jQuery|HTMLElement}
 */
$.fn.fixedTableHeaderResize = function(){
    var table = $(this);
    var fixedTable = $('#fixedTable');
    fixedTable.find("th").each(function (i) {
        var tThWidth = table.find("th").eq(i).width();
        $(this).width(tThWidth);
    });
};