package com.customs.network.fdapn.service;

import com.customs.network.fdapn.dto.CustomerFdaPnFailure;
import com.customs.network.fdapn.dto.ExcelResponse;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Component;

import javax.xml.bind.JAXBException;
import java.util.*;

import static com.customs.network.fdapn.model.MessageCode.REJECT;
import static com.customs.network.fdapn.model.MessageCode.SUCCESS_SUBMIT;
@Component
@AllArgsConstructor
public class ExcelJsonProcessor {
    private final FdaPnRecordSaver fdaPnRecordSaver;
    public Map<String, List<Object>> processResponses(List<ExcelResponse> excelResponses) {
        Map<String, List<Object>> result = new HashMap<>();
        result.put(SUCCESS_SUBMIT.getStatus(), new ArrayList<>());
        result.put(REJECT.getStatus(), new ArrayList<>());

        excelResponses.stream().filter(Objects::nonNull).forEach(excelResponse -> {
            if (excelResponse.getValidationErrors().isEmpty()) {
                fdaPnRecordSaver.save(excelResponse);
                try {
                    result.get(SUCCESS_SUBMIT.getStatus()).add(XmlConverterService.convertToXml(excelResponse.getTrackingDetails()));
                } catch (JAXBException e) {
                    throw new RuntimeException(e);
                }
            } else {
                CustomerFdaPnFailure customerFdaPnFailure = fdaPnRecordSaver.failureRecords(excelResponse);
                result.get(REJECT.getStatus()).add(customerFdaPnFailure);
            }
        });
        return result;
    }

}