package application.exam;

import application.configuration.ApplicationProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ExamWriterConfiguration {
    private final DateRetriever dateRetriever;

    @Autowired
    public ExamWriterConfiguration(ApplicationProperties applicationProperties) {
        this(new DateRetriever(applicationProperties));
    }

    public ExamWriterConfiguration(DateRetriever dateRetriever) {
        this.dateRetriever = dateRetriever;
    }

    @Bean
    public ExamWriter latexWriter() {
        return new ExamWriter(dateRetriever.getDate());
    }
}
