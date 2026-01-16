package com.customs.network.fdapn.service;

import com.customs.network.fdapn.dto.ExcelResponse;
import com.customs.network.fdapn.model.CustomerDetails;
import com.customs.network.fdapn.model.ValidationError;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;

import javax.xml.bind.JAXBException;
import java.util.List;

@Service
@AllArgsConstructor
public class JsonToXmlService {

    private final ValidationService validationService;
    private final FdaPnRecordSaver fdaPnRecordSaver;

    public Object convertJsonToXml(CustomerDetails customerDetails) throws JAXBException {
        ExcelResponse excelResponse = new ExcelResponse();
        excelResponse.setCustomerDetails(customerDetails);

        List<ValidationError> validationErrors = validationService.validateField(List.of(customerDetails));
        excelResponse.setValidationErrors(validationErrors);

        if (!excelResponse.getValidationErrors().isEmpty()) {
            return fdaPnRecordSaver.failureRecords(excelResponse);
        }
        fdaPnRecordSaver.save(excelResponse);
        return XmlConverterService.convertToXml(excelResponse.getCustomerDetails());
    }
}