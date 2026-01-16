package gunes.cashcardtransactionsink.sink;


import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import gunes.domain.*;

import java.util.function.Consumer;

@Configuration
public class CashCardTransactionSink {

    @Bean
    public Consumer<EnrichedTransaction> sinkToConsole(){
        return enrichedTransaction -> {
            System.out.println("Transaction recieved: "+enrichedTransaction);
        };
    }
}