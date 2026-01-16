package refooding.api.domain.exchange.repository;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.test.context.jdbc.Sql;
import refooding.api.domain.exchange.entity.Exchange;
import refooding.api.domain.exchange.entity.ExchangeStatus;
import refooding.api.domain.exchange.entity.Region;
import refooding.api.testUtils.EntityManagerUtil;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
@Sql(value = {"classpath:exchange-data.sql"}, executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
class ExchangeRepositoryTest {

    @Autowired
    private TestEntityManager testEntityManager;
    @Autowired
    RegionRepository regionRepository;
    @Autowired
    ExchangeRepository exchangeRepository;

    @Test
    void 식재료_교환글_저장(){
        //given
        long regionId = 17;
        Region region = regionRepository.findById(regionId).orElseThrow();

        String title = "당근 나눔합니당";
        String content = "맛있는 당근을 나눔합니다";
        Exchange exchange = new Exchange(title, content, region);

        //when
        Exchange savedExchange = exchangeRepository.save(exchange);
        EntityManagerUtil.flushAndClearContext(testEntityManager);
        Exchange findExchange = exchangeRepository.findById(savedExchange.getId()).orElseThrow();

        //then
        assertThat(findExchange).isNotNull();
        assertThat(findExchange.getStatus()).isEqualTo(ExchangeStatus.ACTIVE);
    }

}