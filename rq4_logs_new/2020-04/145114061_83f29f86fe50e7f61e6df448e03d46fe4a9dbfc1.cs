namespace Blog.Bll.Dto.Comments {

    public class CommentsQueryDto {
        public int Take {get;set;}
        public int PostId {get;set;}
        public int Skip {get;set;}
    }
}