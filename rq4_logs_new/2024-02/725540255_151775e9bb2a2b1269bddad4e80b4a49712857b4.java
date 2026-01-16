package jimuanco.jimslog.config;

import com.amazonaws.services.s3.AmazonS3Client;
import jimuanco.jimslog.domain.post.PostImage;
import jimuanco.jimslog.domain.post.PostImageRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@EnableScheduling
@RequiredArgsConstructor
@Component
public class Scheduler {

    @Value("${cloud.aws.s3.bucket}")
    private String bucket;

    private final PostImageRepository postImageRepository;
    private final AmazonS3Client amazonS3Client;

    @Scheduled(cron = "0 0 4 * * *", zone = "Asia/Seoul")
    public void deleteUnNecessaryImage() {
        log.info("불필요한 이미지 삭제 스케줄러 작동 시작");

        List<PostImage> postImages = postImageRepository.findAllByPostIdIsNull();

        List<PostImage> deletePostImages = postImages.stream()
                .filter(postImage ->
                        Duration.between(postImage.getCreatedDateTime(), LocalDateTime.now()).toHours() >= 24)
                .peek(postImage -> amazonS3Client.deleteObject(bucket, postImage.getFileName()))
                .collect(Collectors.toList());

        postImageRepository.deleteAllInBatch(deletePostImages);

        log.info("불필요한 이미지 삭제 스케줄러 작동 완료");
    }
}