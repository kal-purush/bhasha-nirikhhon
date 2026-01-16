package tososomaru.wb.ads.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tososomaru.wb.ads.persistence.bids.InMemoryBidRequestsHistoryPersistence;
import tososomaru.wb.ads.persistence.campaigns.InMemoryCampaignsPersistence;
import tososomaru.wb.ads.persistence.companies.InMemoryCompanyPersistence;

@Configuration
public class PersistenceConfiguration {

    @Bean
    public InMemoryBidRequestsHistoryPersistence InMemoryBidRequestsHistoryPersistence() {
        return new InMemoryBidRequestsHistoryPersistence();
    }

    @Bean
    public InMemoryCompanyPersistence inMemoryCompanyPersistence() {
        return new InMemoryCompanyPersistence();
    }

    @Bean
    public InMemoryCampaignsPersistence inMemoryCampaignsPersistence() {
        return new InMemoryCampaignsPersistence();
    }
}