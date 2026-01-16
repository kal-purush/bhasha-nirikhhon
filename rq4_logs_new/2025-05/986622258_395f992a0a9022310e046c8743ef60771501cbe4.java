package gunes.cashcard.stream;

import gunes.cashcard.domain.Transaction;
import gunes.cashcard.service.DataSourceService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.Supplier;

@Configuration
public class CashCardStream {

    @Bean
    public Supplier<Transaction> approvalRequest(DataSourceService dataSource){
        return ()->{
            return dataSource.getData();
        };
    }

    @Bean
    public DataSourceService dataSourceService(){
        return new DataSourceService();
    }
}