package uk.gov.hmcts.reform.ethos.ecm.consumer.service;

import static uk.gov.hmcts.ecm.common.model.helper.Constants.EMPLOYMENT;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientResponseException;

import com.google.common.collect.Maps;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import uk.gov.hmcts.ecm.common.client.CcdClient;
import uk.gov.hmcts.ecm.common.exceptions.CaseCreationException;
import uk.gov.hmcts.ecm.common.model.servicebus.datamodel.LegalRepDataModel;
import uk.gov.hmcts.et.common.model.ccd.items.ListTypeItem;
import uk.gov.hmcts.et.common.model.ccd.types.SubCaseLegalRepDetails;
import uk.gov.hmcts.et.common.model.multiples.MultipleData;
import uk.gov.hmcts.et.common.model.multiples.SubmitMultipleEvent;

@Slf4j
@RequiredArgsConstructor
@Service
public class LegalRepAccessService {

    private final CcdClient ccdClient;
    private final UserService userService;

    public void run(LegalRepDataModel data) throws IOException {
        log.info("Giving permissions to legalreps for Multiple case");
        String accessToken = userService.getAccessToken();

        SubmitMultipleEvent submitEvent = ccdClient.getMultipleByReference(accessToken, data.getCaseType(), data.getMultipleReference());
        MultipleData multipleData = submitEvent.getCaseData();
        var legalRepsByCaseId = data.getLegalRepIdsByCase();

        if (multipleData.getLegalRepCollection() == null) {
            multipleData.setLegalRepCollection(new ListTypeItem<SubCaseLegalRepDetails>());
        }

        HashMap<String, Boolean> processedIds = new HashMap<>();

        for (Entry<String, List<String>> byCase : legalRepsByCaseId.entrySet()) {
            
            for (String userId : byCase.getValue()){
                updateLegalRepCollection(multipleData.getLegalRepCollection(), byCase.getKey(), userId);

                if (processedIds.containsKey(userId)) {
                    continue;
                }

                processedIds.put(userId, true);

                addUserToMultiple(accessToken, EMPLOYMENT, data.getCaseType(), data.getMultipleReference(), userId);
            }
        }
    }

    private void updateLegalRepCollection(ListTypeItem<SubCaseLegalRepDetails> legalReps, String caseRef, String id) {
        Optional<SubCaseLegalRepDetails> subCase = legalReps.findFirst(o -> caseRef.equals(o.getCaseReference()));
        
        if (subCase.isPresent()) {
            legalReps.addAsItem(SubCaseLegalRepDetails.builder()
                .caseReference(caseRef)
                .legalRepIds(ListTypeItem.from(id))
                .build());

        } else {
            subCase.get().getLegalRepIds().addDistinct(id);
        }
    }

    public void addUserToMultiple(String adminUserToken,
                                    String jurisdiction,
                                    String caseType,
                                    String multipleId,
                                    String userToAddId) throws IOException {
        Map<String, String> payload = Maps.newHashMap();
        payload.put("id", userToAddId);

        String errorMessage = String.format("Call to add legal rep to Multiple Case failed for %s", multipleId);

        try {
            ResponseEntity<Object> response =
                    ccdClient.addUserToMultiple(
                            adminUserToken,
                            jurisdiction,
                            caseType,
                            multipleId,
                            payload);

            if (response == null) {
                throw new CaseCreationException(errorMessage);
            }

            log.info("Http status received from CCD addUserToMultiple API; {}", response.getStatusCodeValue());
        } catch (RestClientResponseException e) {
            throw (CaseCreationException)
                    new CaseCreationException(String.format("%s with %s", errorMessage, e.getMessage())).initCause(e);
        }
    }

}