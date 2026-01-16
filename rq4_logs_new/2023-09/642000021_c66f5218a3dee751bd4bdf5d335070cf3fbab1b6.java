package uk.ac.ebi.ddi;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.support.ClassPathXmlApplicationContext;

@Slf4j
@SpringBootApplication(exclude={DataSourceAutoConfiguration.class})
public class OmicsIndexerPipelineApplication
  implements CommandLineRunner {

    public static void main(String[] args) {
        System.exit(SpringApplication.exit(
                new SpringApplicationBuilder(OmicsIndexerPipelineApplication.class)
                        .web(WebApplicationType.NONE) // .REACTIVE, .SERVLET
                        .run(args)
        ));
    }
 
    @Override
    public void run(String... args) {
        String[] springConfig = {"classpath*:context/mongo-db-context.xml","classpath*:context/common-context.xml","classpath*:context/email-context.xml","classpath*:context/email-context.xml","classpath*:context/spring-data-mongo.xml",args[0]};
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(springConfig);
        JobLauncher jobLauncher = (JobLauncher) context.getBean("jobLauncher");
        Job job = (Job) context.getBean(args[1]);

        JobParameters jobParameters = new JobParametersBuilder().addLong("time", System.currentTimeMillis())
                .toJobParameters();
        try {
            JobExecution execution = jobLauncher.run(job, jobParameters);
        } catch (Exception e) {
            e.printStackTrace();
        }

        context.close();
    }
}