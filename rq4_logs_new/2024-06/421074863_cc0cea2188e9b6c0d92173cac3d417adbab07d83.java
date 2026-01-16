package uk.gov.hmcts.reform.ethos.ecm.consumer.service;

import com.beust.jcommander.internal.Maps;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.gov.hmcts.ecm.common.client.CcdClient;
import uk.gov.hmcts.ecm.common.exceptions.CaseCreationException;
import uk.gov.hmcts.ecm.common.model.servicebus.datamodel.LegalRepDataModel;
import uk.gov.hmcts.et.common.model.ccd.items.ListTypeItem;
import uk.gov.hmcts.et.common.model.ccd.types.SubCaseLegalRepDetails;
import uk.gov.hmcts.et.common.model.multiples.MultipleData;
import uk.gov.hmcts.et.common.model.multiples.MultipleDetails;
import uk.gov.hmcts.et.common.model.multiples.SubmitMultipleEvent;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.naming.NameNotFoundException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertThrows;
import static uk.gov.hmcts.ecm.common.model.helper.Constants.EMPLOYMENT;
import static uk.gov.hmcts.ecm.common.model.helper.Constants.ENGLANDWALES_BULK_CASE_TYPE_ID;

@ExtendWith(SpringExtension.class)
class LegalRepAccessServiceTest {
    @Mock
    CcdClient ccdClient;
    @Mock
    UserService userService;
    @InjectMocks
    LegalRepAccessService legalRepAccessService;

    private static final String ACCESS_TOKEN = "ey123";
    private static final String USER_ID_1 = "d368ec1b-14a0-4e60-9fe1-0ea8263950ea";
    private static final String USER_ID_2 = "7ff82f3f-5e4c-439c-b34c-280086b62377";
    private MultipleDetails details;

    @BeforeEach
    void setUp() {
        when(userService.getAccessToken()).thenReturn(ACCESS_TOKEN);
        details = new MultipleDetails();
        details.setCaseTypeId(ENGLANDWALES_BULK_CASE_TYPE_ID);
        details.setJurisdiction(EMPLOYMENT);
        details.setCaseId("171922876900");
        details.setCaseData(new MultipleData());
        details.getCaseData().setMultipleName("MultipleName");
    }

    @Test
    void addUserToMultiple() throws IOException {
        String token = "ey1234";

        Map<String, String> expectedPayload = Maps.newHashMap();
        expectedPayload.put("id", USER_ID_1);

        when(ccdClient.addUserToMultiple(any(), any(), any(), any(), any()))
            .thenReturn(ResponseEntity.ok(new Object()));

        legalRepAccessService.addUserToMultiple(token,
                                                details.getJurisdiction(), details.getCaseTypeId(),
                                                details.getCaseId(), USER_ID_1);

        verify(ccdClient, times(1)).addUserToMultiple(token,
                                                      details.getJurisdiction(),
                                                      details.getCaseTypeId(),
                                                      details.getCaseId(),
                                                      expectedPayload
        );
    }

    @Test
    void addUserToMultipleThrowsOnError() throws IOException {
        String token = "ey1234";

        Map<String, String> expectedPayload = Maps.newHashMap();
        expectedPayload.put("id", USER_ID_1);

        assertThrows("Call to add legal rep to Multiple Case failed for 171922876900",
                     CaseCreationException.class,
                     () -> legalRepAccessService.addUserToMultiple(token, details.getJurisdiction(),
                        details.getCaseTypeId(), details.getCaseId(), USER_ID_1)
        );

        verify(ccdClient, times(1)).addUserToMultiple(token,
                                                      details.getJurisdiction(),
                                                      details.getCaseTypeId(),
                                                      details.getCaseId(),
                                                      expectedPayload
        );
    }

    @Test
    void run() throws NameNotFoundException, IOException {
        SubmitMultipleEvent event = new SubmitMultipleEvent();
        MultipleData caseData = details.getCaseData();
        event.setCaseData(caseData);

        String singleRef1 = "6000001/2024";
        
        SubCaseLegalRepDetails subCaseLegalRepDetails = SubCaseLegalRepDetails.builder()
            .caseReference(singleRef1)
            .legalRepIds(ListTypeItem.from(USER_ID_1))
            .build();

        caseData.setLegalRepCollection(ListTypeItem.from(subCaseLegalRepDetails));

        when(ccdClient.getMultipleByName(ACCESS_TOKEN, details.getCaseTypeId(), caseData.getMultipleName()))
            .thenReturn(event);

        when(ccdClient.addUserToMultiple(any(), any(), any(), any(), any()))
            .thenReturn(ResponseEntity.ok(new Object()));

        Map<String, List<String>> legalReps = Maps.newHashMap();

        legalReps.put(singleRef1, List.of(USER_ID_2));
        legalReps.put("6000002/2024", List.of(USER_ID_2));

        var dataModel = LegalRepDataModel.builder()
            .caseType(details.getCaseTypeId())
            .legalRepIdsByCase(legalReps)
            .multipleName(caseData.getMultipleName())
            .build();

        legalRepAccessService.run(dataModel);

        verify(ccdClient, times(1)).submitMultipleEventForCase(eq(ACCESS_TOKEN), any(), any(), any(), any(), any());
    }
}