using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace Fashion.Models
{
    public class PostFull_model
    {
        //帖子完整信息
        public int postId;//帖子编码
        public string postCaption;//帖子标题
        public string postContent;//帖子内容
        public string postSender;//发布者
        public string touXiangUrl;//发布者头像url
        public string signature;//个性签名
        public int postModule;//模块
        public int postTheme;//话题
        public int postSupportCount;//点赞数
        public DateTime postDate;//发帖日期
    }
}