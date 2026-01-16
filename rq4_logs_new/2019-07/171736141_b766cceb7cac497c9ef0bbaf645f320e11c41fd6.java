package ru.otus.batchConfig;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.builder.MongoItemReaderBuilder;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import ru.otus.domain.jdbc.BookAuthor;
import ru.otus.domain.jdbc.BookGenre;
import ru.otus.domain.jdbc.MigrationBook;
import ru.otus.domain.mongo.Author;
import ru.otus.domain.mongo.Book;
import ru.otus.mapper.MongoToJdbcService;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.List;

@EnableBatchProcessing
@Configuration
@RequiredArgsConstructor(onConstructor = @__({@Autowired}))
public class BatchConfig {

    private Logger logger = LoggerFactory.getLogger(BatchConfig.class);

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    @Bean
    public ItemReader<Author> authorReader(MongoTemplate mongoTemplate) {
        return new MongoItemReaderBuilder<Author>()
                .name("readMongoAuthors")
                .jsonQuery("{}")
                .template(mongoTemplate)
                .sorts(new HashMap<>())
                .targetType(Author.class)
                .build();
    }

    @Bean
    public ItemProcessor<Author, BookAuthor> authorProcessor(MongoToJdbcService mapper) {
        return mapper::toBookAuthor;
    }

    @Bean
    public JdbcBatchItemWriter jdbcAuthorWriter(DataSource dataSource) {
        JdbcBatchItemWriter writer = new JdbcBatchItemWriter();
        writer.setDataSource(dataSource);
        writer.setSql("insert into author (AUTHOR_NAME) values (:authorName)");
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider());
        return writer;
    }

    @Bean
    public ItemReader<Book> genreReader(MongoTemplate mongoTemplate) {
        return new MongoItemReaderBuilder<Book>()
                .name("readMongoAuthors")
                .jsonQuery("{}")
                .template(mongoTemplate)
                .sorts(new HashMap<>())
                .targetType(Book.class)
                .build();
    }

    @Bean
    public ItemProcessor<Book, BookGenre> genreProcessor(MongoToJdbcService mapper) {
        return mapper::toBookGenre;
    }

    @Bean
    public JdbcBatchItemWriter jdbcGenreWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<>()
                .dataSource(dataSource)
                .sql("insert into genre (GENRE_NAME) values (:genre)")
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider())
                .build();
    }

    @Bean
    public ItemReader<Book> bookReader(MongoTemplate mongoTemplate) {
        return new MongoItemReaderBuilder<Book>()
                .name("readMongoAuthors")
                .jsonQuery("{}")
                .template(mongoTemplate)
                .sorts(new HashMap<>())
                .targetType(Book.class)
                .build();
    }

    @Bean
    public ItemProcessor<Book, MigrationBook> bookProcessor(MongoToJdbcService mapper) {
        return mapper::toMigrationBook;
    }

    @Bean
    public JdbcBatchItemWriter jdbcBookWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<>()
                .dataSource(dataSource)
                .sql("insert into books (TITLE, GENRE_ID, AUTHOR_ID) values (:title, :genreId, :authorId)")
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider())
                .build();
    }

    @Bean
    public Job migrateJob(Step migrateAuthors, Step migrateGenres, Step migrateBooks) {
        return jobBuilderFactory.get("mongoMigrateToJdbc")
                .incrementer(new RunIdIncrementer())
                .start(migrateAuthors)
                .next(migrateGenres)
                .next(migrateBooks)
                .build();
    }

    @Bean
    @SuppressWarnings("unchecked")
    public Step migrateAuthors(ItemReader authorReader, ItemProcessor authorProcessor, ItemWriter jdbcAuthorWriter) {
        return stepBuilderFactory.get("mongoMigrateAuthorsToJdbc")
                .chunk(5)
                .reader(authorReader)
                .processor(authorProcessor)
                .writer(jdbcAuthorWriter)
                .listener(new ItemReadListener() {
                    @Override
                    public void beforeRead() {
                    }

                    @Override
                    public void afterRead(Object o) {
                        logger.info("Read {}", o);
                    }

                    @Override
                    public void onReadError(Exception e) {

                    }
                })
                .listener(new ItemProcessListener() {
                    @Override
                    public void beforeProcess(Object o) {
                        logger.info("Before convert {} class {}", o, o.getClass());
                    }

                    @Override
                    public void afterProcess(Object o, Object o2) {
                        logger.info("After convert from {} class {} to {} class {}", o, o.getClass(), o2, o2.getClass());
                    }

                    @Override
                    public void onProcessError(Object o, Exception e) {

                    }
                })
                .listener(new ItemWriteListener() {
                    @Override
                    public void beforeWrite(List list) {
                        logger.info("Before write {}", list);
                    }

                    @Override
                    public void afterWrite(List list) {
                        logger.info("After write {}", list);
                    }

                    @Override
                    public void onWriteError(Exception e, List list) {
                    }
                })
                .build();
    }

    @Bean
    @SuppressWarnings("unchecked")
    public Step migrateGenres(ItemReader genreReader, ItemProcessor genreProcessor, ItemWriter jdbcGenreWriter) {
        return stepBuilderFactory.get("mongoMigrateGenresToJdbc")
                .chunk(5)
                .reader(genreReader)
                .processor(genreProcessor)
                .writer(jdbcGenreWriter)
                .listener(new ItemReadListener() {
                    @Override
                    public void beforeRead() {
                    }

                    @Override
                    public void afterRead(Object o) {
                        logger.info("Read {}", o);
                    }

                    @Override
                    public void onReadError(Exception e) {

                    }
                })
                .listener(new ItemProcessListener() {
                    @Override
                    public void beforeProcess(Object o) {
                        logger.info("Before convert {} class {}", o, o.getClass());
                    }

                    @Override
                    public void afterProcess(Object o, Object o2) {
                        logger.info("After convert from {} class {} to {} class {}", o, o.getClass(), o2, o2.getClass());
                    }

                    @Override
                    public void onProcessError(Object o, Exception e) {

                    }
                })
                .listener(new ItemWriteListener() {
                    @Override
                    public void beforeWrite(List list) {
                        logger.info("Before write {}", list);
                    }

                    @Override
                    public void afterWrite(List list) {
                        logger.info("After write {}", list);
                    }

                    @Override
                    public void onWriteError(Exception e, List list) {
                    }
                })
                .build();
    }

    @Bean
    @SuppressWarnings("unchecked")
    public Step migrateBooks(ItemReader bookReader, ItemProcessor bookProcessor, ItemWriter jdbcBookWriter) {
        return stepBuilderFactory.get("mongoMigrateGenresToJdbc")
                .chunk(5)
                .reader(bookReader)
                .processor(bookProcessor)
                .writer(jdbcBookWriter)
                .listener(new ItemReadListener() {
                    @Override
                    public void beforeRead() {
                    }

                    @Override
                    public void afterRead(Object o) {
                        logger.info("Read {}", o);
                    }

                    @Override
                    public void onReadError(Exception e) {

                    }
                })
                .listener(new ItemProcessListener() {
                    @Override
                    public void beforeProcess(Object o) {
                        logger.info("Before convert {} class {}", o, o.getClass());
                    }

                    @Override
                    public void afterProcess(Object o, Object o2) {
                        logger.info("After convert from {} class {} to {} class {}", o, o.getClass(), o2, o2.getClass());
                    }

                    @Override
                    public void onProcessError(Object o, Exception e) {

                    }
                })
                .listener(new ItemWriteListener() {
                    @Override
                    public void beforeWrite(List list) {
                        logger.info("Before write {}", list);
                    }

                    @Override
                    public void afterWrite(List list) {
                        logger.info("After write {}", list);
                    }

                    @Override
                    public void onWriteError(Exception e, List list) {
                    }
                })
                .build();
    }


}