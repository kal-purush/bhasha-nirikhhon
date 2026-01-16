$( document ).ready(function() {
    var tag_list = eval($("#tag_cloud").data('tags').split(';'));
    tag_list.pop();

    var tag_json = [];
    var total_cnt = 0;
    $.each(tag_list, function( idx, val ) {
        foo = eval(val);
        tag_json.push(foo);
        total_cnt += foo[1];
    })
    generate_tag_cloud(tag_json, total_cnt);
});

var generate_tag_cloud = function(tag_json, total_cnt) {
    tag_cloud_str = "<div id='tag_cloud_canvas'>";

    $.each(tag_json, function(idx, val) {
        font_size = 8 + Math.log(val[1])/Math.log(total_cnt) * 18;
        tag_cloud_str += "<a style=\"font-size:" + font_size +"pt\" href=" + val[2] + ">" + val[0] + "</a>&nbsp; ";
    });

    tag_cloud_str += "</div>";

    $("#tag_cloud").append(tag_cloud_str);
}