package com.example.samplebatch.batch;

import com.example.samplebatch.entity.BeforeEntity;
import com.example.samplebatch.repository.BeforeRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.item.data.RepositoryItemReader;
import org.springframework.batch.item.data.builder.RepositoryItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.FieldExtractor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.data.domain.Sort;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class FifthBatch {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;
    private final BeforeRepository beforeRepository;

    @Bean
    public Job fifthJob() {

        return new JobBuilder("fifthJob", jobRepository)
                .start(fifthStep())
                .build();
    }

    @Bean
    public Step fifthStep() {

        return new StepBuilder("fifthStep", jobRepository)
                .<BeforeEntity, BeforeEntity> chunk(10, platformTransactionManager)
                .reader(fifthBeforeReader())
                .processor(fifthProcessor())
                .writer(txtFileWriter())
                .build();
    }

    @Bean
    public RepositoryItemReader<BeforeEntity> fifthBeforeReader() {

        RepositoryItemReader<BeforeEntity> reader = new RepositoryItemReaderBuilder<BeforeEntity>()
                .name("beforeReader")
                .pageSize(10)
                .methodName("findAll")
                .repository(beforeRepository)
                .sorts(Map.of("id", Sort.Direction.ASC))
                .build();

        // 전체 데이터 셋에서 어디까지 수행 했는지의 값을 저장하지 않음
        // 엑셀 to DB와 달리 DB to 엑셀은 작업이 중간에 중단되면 파일을 새로 생성해야 하므로
        // ExcelRowReader의 cursor 저장과 같은 로직의 아래 코드를 false로 한다.
        reader.setSaveState(false);

        return reader;
    }

    @Bean
    public ItemProcessor<BeforeEntity, BeforeEntity> fifthProcessor() {
        return item -> item;
    }

    @Bean
    public ItemStreamWriter<BeforeEntity> txtFileWriter() {
        FlatFileItemWriter<BeforeEntity> writer = new FlatFileItemWriter<>();

        // 파일 경로 설정
        writer.setResource(new FileSystemResource("C:\\Users\\User\\Desktop\\result.txt"));

        // 각 엔티티를 어떻게 문자열로 변환할지 설정
        DelimitedLineAggregator<BeforeEntity> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter("|");

        // 엔티티에서 필드를 추출하는 방법 설정
        lineAggregator.setFieldExtractor(new FieldExtractor<BeforeEntity>() {
            @Override
            public Object[] extract(BeforeEntity item) {
                // BeforeEntity의 필드를 배열로 반환
                return new Object[] {
                         item.getId()
                        ,item.getUsername()
                };
            }
        });

        writer.setLineAggregator(lineAggregator);

        return writer;
    }
}