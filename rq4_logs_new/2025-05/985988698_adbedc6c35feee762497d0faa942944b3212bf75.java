package com.biopatternsg.domain.port;

import com.biopatternsg.domain.models.BlatSearchOptions;
import com.biopatternsg.infrastructure.dtos.PromoterRegionDTO;

import java.util.List;

public interface BlatProviderPort {
    List<BlatSearchOptions> fetchDataFromBlatSource(PromoterRegionDTO promoterRegionDTO);
}