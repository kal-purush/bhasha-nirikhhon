package refooding.api.domain.exchange.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import refooding.api.domain.exchange.dto.RegionResponse;
import refooding.api.domain.exchange.entity.Region;
import refooding.api.domain.exchange.repository.RegionRepository;

import java.util.List;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class RegionService {

    private final RegionRepository regionRepository;

    public List<RegionResponse> getRegionsWithChildren() {
        List<Region> regions = regionRepository.findByParentIsNull();
        return regions
                .stream()
                .map(RegionResponse::from)
                .toList();
    }

}