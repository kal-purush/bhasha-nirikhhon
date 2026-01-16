package cotato.growingpain.post.dto.response;

import cotato.growingpain.post.domain.entity.PostImage;
import java.util.List;

public record PostImageListResponse (
    List <PostImageResponse> postImages
) {
    public static PostImageListResponse from(List<PostImage> postImages) {
        List <PostImageResponse> postImageResponses = postImages.stream()
                .map(PostImageResponse::from)
                .toList();
        return new PostImageListResponse(postImageResponses);
    }
}