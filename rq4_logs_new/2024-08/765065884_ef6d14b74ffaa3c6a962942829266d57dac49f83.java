package it.gov.pagopa.wispconverter.service;

import it.gov.pagopa.wispconverter.repository.RTRepository;
import it.gov.pagopa.wispconverter.repository.model.RTEntity;
import it.gov.pagopa.wispconverter.repository.model.enumz.ReceiptTypeEnum;
import it.gov.pagopa.wispconverter.service.model.session.RPTContentDTO;
import it.gov.pagopa.wispconverter.service.model.session.SessionDataDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;


@Service
@Slf4j
@RequiredArgsConstructor
public class RtReceiptCosmosService {

    private final RTRepository rtRepository;

    @Transactional
    public void saveRTEntity(SessionDataDTO sessionData) {
        rtRepository.save(getRTEntity(sessionData, null, null));
    }

    @Transactional
    public void saveRTEntity(SessionDataDTO sessionData, String rt, ReceiptTypeEnum receiptType) {
        rtRepository.save(getRTEntity(sessionData, rt, receiptType));
    }

    private RTEntity getRTEntity(SessionDataDTO sessionData, String rt, ReceiptTypeEnum receiptType) {
        RPTContentDTO rptContentDTO = sessionData.getFirstRPT();
        String IUV = rptContentDTO.getIuv();
        String ccp = rptContentDTO.getCcp();
        String domainId = rptContentDTO.getRpt().getDomain().getDomainId();

        String id = String.format("%s_%s_%s", IUV, ccp, domainId);

        return RTEntity.builder()
                .id(id)
                .partitionKey(id)
                .iuv(IUV)
                .ccp(ccp)
                .idDominio(domainId)
                .rt(rt)
                .receiptType(receiptType)
                .build();
    }
}