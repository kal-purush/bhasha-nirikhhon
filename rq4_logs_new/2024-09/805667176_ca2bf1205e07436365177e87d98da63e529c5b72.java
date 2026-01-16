package cotato.growingpain.post.domain.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class PostImage {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "post_image_id")
    private Long id;


    @Column(name = "post_id", nullable = false)
    private Long postId;

    @Column(name = "post_image_url", nullable = false)
    private String imageUrl;

    public PostImage(Long postId, String imageUrl) {
        this.postId = postId;
        this.imageUrl = imageUrl;
    }
}