package refooding.api.domain.exchange.repository;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;
import refooding.api.domain.exchange.entity.Region;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Transactional
class RegionRepositoryTest {

    @Autowired
    RegionRepository regionRepository;

    @Test
    void 식재료_교환_지역_조회() {
        // when
        List<Region> parentRegions = regionRepository.findByParentIsNull();

        //then
        assertThat(parentRegions).isNotNull();
        assertThat(parentRegions.get(0).getParent()).isNull();
        assertThat(parentRegions.get(0).getName()).isNotNull();
        assertThat(parentRegions.get(0).getChildren()).isNotEmpty();
    }

}