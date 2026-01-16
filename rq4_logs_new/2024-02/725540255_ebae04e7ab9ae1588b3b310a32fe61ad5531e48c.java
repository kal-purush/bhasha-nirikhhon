package jimuanco.jimslog.config;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import jimuanco.jimslog.IntegrationTestSupport;
import jimuanco.jimslog.domain.post.PostImage;
import jimuanco.jimslog.domain.post.PostImageRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.io.ByteArrayInputStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SchedulerTest extends IntegrationTestSupport {

    @Autowired
    private PostImageRepository postImageRepository;

    @Autowired
    private AmazonS3 amazonS3;

    @Autowired
    private Scheduler scheduler;

    @Value("${cloud.aws.s3.bucket}")
    private String bucket;

    @BeforeEach
    public void setUp() {
        amazonS3.createBucket(bucket);
    }

    @AfterEach
    public void tearDown() {
        amazonS3.deleteBucket(bucket);
    }

    @DisplayName("일정 시간 이상 등록되지 않은 글의 이미지는 스케줄러에 의해 삭제된다.")
    @Test
    void deletePostImagesWhenTimeExceeds() {
        // given
        String fileName = "images/image.png";
        PostImage postImage = PostImage.builder()
                .fileName(fileName)
                .build();
        postImageRepository.save(postImage);

        String contentType = "image/png";
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentType(contentType);
        PutObjectRequest putObjectRequest
                = new PutObjectRequest(bucket, fileName, new ByteArrayInputStream("".getBytes(UTF_8)), objectMetadata);
        amazonS3.putObject(putObjectRequest);

        // when
        scheduler.deleteUnNecessaryImage();

        // then
        assertThat(postImageRepository.findAll().size()).isEqualTo(0);
        assertThatThrownBy(() -> amazonS3.getObject(bucket, fileName))
                .isInstanceOf(AmazonS3Exception.class);
    }
}