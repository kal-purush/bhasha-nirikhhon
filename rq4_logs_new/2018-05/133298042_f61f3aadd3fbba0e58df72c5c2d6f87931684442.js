//默认新闻渲染
var startIndex = 0;
var pageLimit = 6;
$.ajax({
    type: 'post',
    url: 'https://apix.funinhr.com/api/news/getList',
    dataType: 'json',
    data: { pageStart: startIndex,pageLimit:pageLimit },
    success: function (data) {
        startIndex = startIndex + 6;
        $('.news_out').attr('index', startIndex);
        var contentList = '';
        $(data).each(function (i, v) {
            var cid = v.cid;
            var time = v.time;
            var timeMonth = time.substr(6, 1)
            var timeData = time.substr(8, 2)
            var description = v.description;
            var title = v.title;
            var descriptionLength = description.length;
            var titleLength = title.length;
            var imgUrl = v.imgUrl;
            if (titleLength>60) {
                title = title.substring(0,60) + '...';
            }
            if (descriptionLength>200) {
                description = description.substring(0, 200) + '...';
            }
            var content =
           
            '<li class="news_box" cid="'+cid+'">'+
                        '<div class="news_l">'+
                            '<p class="newsTitle">'+title+'</p>'+
                            '<div class="cutOff">'+'</div>'+
                            '<p class="news_content">'+description+'</p>'+
                            '<span class="news_more">'+'查看全部》'+'</span>'+
                        '</div>'+
                        '<div class="news_r">'+
                            '<img src="'+imgUrl+'" alt="">'+
                        '</div>'+
                    '</li>'
            
            contentList += content;
        })
        $('.news ul').html(contentList);
    }

  
})
$('.news').on('click','.news_more',function(){
    var cid = $(this).parent().parent().attr('cid');
    window.location.href = 'https://api.funinhr.com/api/news/viewNews?cid='+cid;
})


$('.check_btn').click(function () {
    var contentUl;
    $.ajax({
    type: 'post',
    url: 'https://apix.funinhr.com/api/news/getList',
    dataType: 'json',
    data: { pageStart: startIndex,pageLimit:pageLimit },
    success: function (data) {
        startIndex = startIndex + 6;
        $('.news_out').attr('index', startIndex);
        var contentList = '';
        $(data).each(function (i, v) {
            var cid = v.cid;
            var time = v.time;
            var timeMonth = time.substr(6, 1)
            var timeData = time.substr(8, 2)
            var description = v.description;
            var title = v.title;
            var descriptionLength = description.length;
            var titleLength = title.length;
            var imgUrl = v.imgUrl;
            if (titleLength>60) {
                title = title.substring(0,60) + '...';
            }
            if (descriptionLength>200) {
                description = description.substring(0, 200) + '...';
            }
            var content =
           
            '<li class="news_box" cid="'+cid+'">'+
                        '<div class="news_l">'+
                            '<p class="newsTitle">'+title+'</p>'+
                            '<div class="cutOff">'+'</div>'+
                            '<p class="news_content">'+description+'</p>'+
                            '<span class="news_more">'+'查看全部》'+'</span>'+
                        '</div>'+
                        '<div class="news_r">'+
                            '<img src="'+imgUrl+'" alt="">'+
                        '</div>'+
                    '</li>'
            
            contentList += content;
            var contentUl = '<ul>'+contentList+'</ul>'
        })
        $('.news_content').append(contentUl);
    }

    })  
})  