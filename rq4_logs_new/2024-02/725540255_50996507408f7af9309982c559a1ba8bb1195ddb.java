package jimuanco.jimslog.domain.post;

import jimuanco.jimslog.IntegrationTestSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Transactional
class PostImageRepositoryTest extends IntegrationTestSupport {

    @Autowired
    private PostImageRepository postImageRepository;

    @DisplayName("글 이미지를 파일 이름으로 한번에 삭제한다.")
    @Test
    void deleteAllByFileNameInQuery() {
        // given
        String image1 = "image1";
        String image2 = "image2";
        String image3 = "image3";

        PostImage postImage1 = PostImage.builder()
                .fileName(image1)
                .build();
        PostImage postImage2 = PostImage.builder()
                .fileName(image2)
                .build();
        PostImage postImage3 = PostImage.builder()
                .fileName(image3)
                .build();
        postImageRepository.saveAll(List.of(postImage1, postImage2, postImage3));

        // when
        postImageRepository.deleteAllByFileNameInQuery(List.of(image1, image2, image3));

        // then
        assertThat(postImageRepository.findAll()).hasSize(0);

    }

}