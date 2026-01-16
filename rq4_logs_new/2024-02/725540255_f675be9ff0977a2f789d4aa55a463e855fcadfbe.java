package jimuanco.jimslog.api.service.post;

import io.findify.s3mock.S3Mock;
import jimuanco.jimslog.IntegrationTestSupport;
import jimuanco.jimslog.domain.post.PostImage;
import jimuanco.jimslog.domain.post.PostImageRepository;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Transactional
class S3UploaderTest extends IntegrationTestSupport {

    @Autowired
    private S3Uploader s3Uploader;

    @Autowired
    private PostImageRepository postImageRepository;

    @AfterAll
    static void tearDown(@Autowired S3Mock s3Mock) {
        s3Mock.stop();
    }

    @DisplayName("S3에 이미지파일을 업로드 한다.")
    @Test
    void upload() throws IOException {
        // given
        String originalFileName = "image.png";
        String dirName = "images";

        MockMultipartFile image = new MockMultipartFile("postImage",
                originalFileName,
                "image/png",
                "<<image.png>>".getBytes());
        // when
        String uploadImageUrl = s3Uploader.upload(image, dirName);

        // then
        assertThat(uploadImageUrl).contains(originalFileName);
        assertThat(uploadImageUrl).contains(dirName);

        List<PostImage> postImages = postImageRepository.findAll();
        assertThat(postImages).hasSize(1);
        assertThat(postImages.get(0).getPostId()).isNull();
    }
}