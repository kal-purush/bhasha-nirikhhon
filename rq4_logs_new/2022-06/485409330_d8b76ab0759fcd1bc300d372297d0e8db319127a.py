import json
import copy
import EmailDelivered
import DeliveryConstants

test_case_info = {
    "samples": [{
    "cmoId": "A-4158-PY-P-3m-Bas",
    "concentration": "0.0 null",
    "dropOffDate": 0,
    "estimatedPurity": 0,
    "organism": "Mouse",
    "project": "13032_B",
    "recipe": "ATACSeq",
    "userId": "A-4158-PY-P-3m-Bas",
    "volume": 0,
    "yield": 0,
    "numberOfAmplicons": 0,
    "basicQcs": [
    {
    "run": "DIANA_0482_AHLFYJDSX3",
    "sampleName": "A-4158-PY-P-3m-Bas",
    "qcStatus": "Passed",
    "restStatus": "SUCCESS",
    "totalReads": 334868354,
    "createDate": 1653144565916,
    "reviewedDates": [
    {
    "timestamp": 1653403297252,
    "event": "Passed"
    }
    ]
    }
    ],
    "cancerType": "null",
    "expName": "A-4158-PY-P-3m-Bas",
    "vol": 0,
    "baseId": "13032_B_1",
    "species": "Mouse"
    }],
    "requestId": "08749_D",
    "investigator": "Jing Hu",
    "dataAccessEmails": "huj2@mskcc.org",
    "pi": "Joan Massague",
    "investigatorEmail": "huj2@mskcc.org",
    "piEmail": "j-massague@ski.mskcc.org",
    "deliveryDate": 1651846477928,
    "analysisRequested": "true",
    }

test_case = EmailDelivered.DeliveryDescription(test_case_info)

def test_determine_email_recipient_recipt():
    test_case.recipe = "Impact"
    recipient = ["huj2@mskcc.org"]
    runType = EmailDelivered.recipe2RunType(test_case.recipe)
    (toList, ccList) = EmailDelivered.determineDataAccessRecipients(test_case, recipient, runType, copy.deepcopy(DeliveryConstants.addressMap))
    assert (ccList == DeliveryConstants.addressMap["standard"] + DeliveryConstants.addressMap["impact"])
    assert (toList == recipient)

def test_determine_email_recipient_analysisType():
    test_case.recipe = "ATACSeq"
    test_case.analysisType = "BIC"
    recipient = ["huj2@mskcc.org"]
    runType = EmailDelivered.recipe2RunType(test_case.recipe)
    (toList, ccList) = EmailDelivered.determineDataAccessRecipients(test_case, recipient, runType, copy.deepcopy(DeliveryConstants.addressMap))
    assert (ccList == DeliveryConstants.addressMap["standard"] + DeliveryConstants.addressMap["pipelineDefault"])

def test_determine_email_content():
    runType = EmailDelivered.recipe2RunType(test_case.recipe)
    email = EmailDelivered.determineDataAccessContent(test_case, runType)
    assert (email["subject"] == (DeliveryConstants.genericSubject
            % (test_case.recipe, test_case.requestId)))
    assert (email["content"] == (DeliveryConstants.genericAnalysisContent) % (
            test_case.recipe,
            test_case.requestId,
        ) )