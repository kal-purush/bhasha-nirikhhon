package com.ipiecoles.batch.dbExport;



import com.ipiecoles.batch.model.Commune;
import com.ipiecoles.batch.repository.CommuneRepository;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.FormatterLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import javax.persistence.EntityManagerFactory;
import java.sql.SQLException;


@Configuration
@EnableBatchProcessing
public class CommuneDBExportBatch {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    public EntityManagerFactory entityManagerFactory;

    @Autowired
    public CommuneRepository communeRepository;

    @Autowired
    public CommunesDBExportListener communesDBExportListener;


    @Value("10")// ne fonctionne pas avec : "${importFile.chunkSize}"
    private Integer chunkSize;

    /////////////////////////////////////
    ///////// READER ////////////////////
    /////////////////////////////////////


    // lire la bdd et trier par code postal et code insee
    @Bean
    public JpaPagingItemReader<Commune> JpaItemReader() {
        return new JpaPagingItemReaderBuilder<Commune>()
                .name("JpaItemReader")
                .entityManagerFactory(entityManagerFactory)
                .pageSize(10)
                .queryString("from Commune c order by code_postal, code_insee")
                .build();
    }

    ////////////////////////////////////////
    /////////// WRITER /////////////////////
    ///////////////////////////////////////

    // pour écrire le fichier définitif avec le bon formatage
    @Bean
    public ItemWriter<Commune> fileItemWriter() {
        BeanWrapperFieldExtractor<Commune> bwfe = new BeanWrapperFieldExtractor<Commune>();
        bwfe.setNames(new String[]{"codePostal", "codeInsee", "nom", "latitude", "longitude"});

        FormatterLineAggregator<Commune> agg = new FormatterLineAggregator<>();
        agg.setFieldExtractor(bwfe);
        agg.setFormat("%5s - %5s - %s : %.5f %.5f");// formatage du fichier final

        FlatFileItemWriter<Commune> flatFileItemWriter =
                new FlatFileItemWriter<>();
        flatFileItemWriter.setName("txtWriter");
        flatFileItemWriter.setResource(new FileSystemResource("target/test.txt"));
        flatFileItemWriter.setFooterCallback(new FooterCustom(communeRepository)); // creation de la ligne footer
        flatFileItemWriter.setHeaderCallback(new HeaderCustom(communeRepository));//creation de la ligne header
        flatFileItemWriter.setLineAggregator(agg);


        return flatFileItemWriter;
    }

    ///////////////////////////////////////
    //////////// LISTENER /////////////////
    ///////////////////////////////////////

    @Bean
    public CommunesDBExportListener communesDBExportListener() {
        return new CommunesDBExportListener();
    }
    // pour ignorer un element


    ///////////////////////////////////////
    //////////// TASKLET /////////////////
    ///////////////////////////////////////


    // Tasklet
    @Bean
    public Tasklet infoTasklet(){
        return new InfoTasklet();
    }

    @Bean
    public Step stepTasklet() {
        return stepBuilderFactory.get("exportFile")
                .tasklet(infoTasklet())
                .listener(infoTasklet())
                .build();
    }


    @Bean
    public Step stepExport() {
        return stepBuilderFactory.get("exportFile")
                .<Commune, Commune>chunk(chunkSize)
                .reader(JpaItemReader())
                .writer(fileItemWriter())
                .listener(communesDBExportListener())
        // gestion erreur => relance le traitement en cas d'indispo jusqu'à 3 fois
                .faultTolerant()
                .retryLimit(3)
                .retry(SQLException.class)
        // gestion erreur => ignorer les éléments levant une exception
                .skipLimit(10)
                .skip(FlatFileParseException.class)
                .build();
    }

    ///////////////////////////////////
    ////////// JOB ////////////////////
    ///////////////////////////////////

    // job lance les méthodes
    @Bean
    @Qualifier("exportCommunes")
    public Job exportCommunes(Step stepExport) {
        return jobBuilderFactory.get("exportCommunes")
         //       .incrementer(new RunIdIncrementer())
                .flow(stepTasklet())
                .next(stepExport())
                .end().build();
    }


}