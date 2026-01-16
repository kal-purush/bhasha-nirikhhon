package jimuanco.jimslog.domain.post;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jimuanco.jimslog.domain.BaseEntity;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import static jakarta.persistence.GenerationType.IDENTITY;
import static lombok.AccessLevel.PROTECTED;

@Getter
@NoArgsConstructor(access = PROTECTED)
@Entity
public class PostImage extends BaseEntity {

    @Id
    @GeneratedValue(strategy = IDENTITY)
    private Long id;

    private Long postId;

    private String fileName;

    @Builder
    public PostImage(Long postId, String fileName) {
        this.postId = postId;
        this.fileName = fileName;
    }

    public void updatePostId(Long postId) {
        this.postId = postId;
    }

}