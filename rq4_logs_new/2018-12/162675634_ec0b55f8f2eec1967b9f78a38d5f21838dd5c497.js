$(function(){
var id=getQueryString('productid');
console.log(id);

    $.ajax({
        url: "http://localhost:9090/api/getmoneyctrlproduct",
        data: {productid:id},
        success: function (data) {
            var html=template('productDetailTpl',data);
            $('.productDetail').html(html);
            console.log(html);
        }
    });


    
        // 根据url参数名取值
        function getQueryString(name) {
            var reg = new RegExp('(^|&)' + name + '=([^&]*)(&|$)', 'i');
            var r = window.location.search.substr(1).match(reg);
            // console.log(r);
            if (r != null) {
                //转码方式改成 decodeURI
                return decodeURI(r[2]);
            }
            return null;
        }
});