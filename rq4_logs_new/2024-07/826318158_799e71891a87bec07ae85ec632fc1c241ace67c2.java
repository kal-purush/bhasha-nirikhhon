package nus.mini.backend.controllers;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.cloud.documentai.v1.Document;
import com.google.cloud.documentai.v1.Document.Entity;
import com.google.cloud.documentai.v1.DocumentProcessorServiceClient;
import com.google.cloud.documentai.v1.ProcessRequest;
import com.google.cloud.documentai.v1.ProcessResponse;
import com.google.cloud.documentai.v1.ProcessorName;
import com.google.cloud.documentai.v1.RawDocument;

import nus.mini.backend.jdbcrepositories.ReceiptRepository;
import nus.mini.backend.models.EnumTypes;
import nus.mini.backend.models.Receipt;
import nus.mini.backend.utilities.DateTimeParser;

@Service
public class ImgAIService {

    // @Autowired
    // private GoogleAIService documentAIService;

    @Autowired
    private ReceiptRepository receiptRepository;

    private static final String PROJECT_ID = "neat-shell-428904-j4";
    private static final String LOCATION = "us"; // e.g., "us"
    private static final String PROCESSOR_ID = "f0907bec67c693f3";

    public Integer handleFileUpload(int user, String fileName, 
                String contentType, byte[] content) throws SQLException, IOException {
            // System.out.println("handleFileUpload >>> userId: "+user);
            // System.out.println("handleFileUpload >>> fileName: "+fileName);
            // System.out.println("handleFileUpload >>> contentType: "+contentType);
            Document document = processDocument(content, contentType);
           // create a new Receipt record in MySql
            System.out.println("handleFileUpload >>> document: ");
            Receipt extractedReceipt = extractDataFromDocument(document, user, fileName);
            System.out.println("handleFileUpload >>> receipt: "+extractedReceipt.toString());
            return receiptRepository.save(extractedReceipt);
    }

    //process the uploaded file using Google Document AI
    //assuming MultipartFile.getContentType() uses same format as RawDocument.mimeType
    public Document processDocument(byte[] fileContent, String mineType) throws IOException {
        System.out.println("ZZZZZZZZZZZZZZZZZZZZzBefore costructing AI ProcessorName >>>  ");
        try (DocumentProcessorServiceClient client = DocumentProcessorServiceClient.create()) {
            System.out.println("Before costructing AI ProcessorName >>>  ");
            // https://eu-documentai.googleapis.com/v1/projects/$PROJECT_ID/locations/eu/processors/$PROCESSOR_ID:process
            ProcessorName processorName = ProcessorName.of(PROJECT_ID, LOCATION, PROCESSOR_ID);
            System.out.println("processDocument >>> processorName: "+processorName.toString());
            ProcessRequest request = ProcessRequest.newBuilder()
                .setName(processorName.toString())
                .setRawDocument(RawDocument.newBuilder()
                    .setContent(com.google.protobuf.ByteString.copyFrom(fileContent))
                    .setMimeType(mineType)
//                     .setMimeType("image/jpeg")
//    //                 .setMimeType("application/pdf")
                    .build())
                .build();          
            System.out.println("processDocument >>> request: ");
            ProcessResponse result = client.processDocument(request);
            return result.getDocument();
        }
    }
    
    
    //extract data from the Document object into a Receipt object and save it to MySql
    private Receipt extractDataFromDocument(Document document,
                int userId, String fileName) throws SQLException, IOException {
        Map<String, String> dataMap = new HashMap<>();
        for (Entity entity : document.getEntitiesList()) {
             dataMap.put(entity.getType(), entity.getMentionText());
            // dataMap.put(entity.getEntityType().name(), entity.getText());
                //       dataMap.put(entity.getType(), entity.getValue());
        }
        System.out.println("extractDataFromDocument dataMap >>>>>>>: " + dataMap.toString());
        LocalDateTime dateTime = DateTimeParser.parseDateTime(
                dataMap.get("receipt_date"), dataMap.get("purchase_time"));
         return Receipt.builder()
                .userId(userId)
                .fileUrl(fileName)
                .uploadTime(LocalDateTime.now())
                .payer(dataMap.get("credit_card_last_four_digits"))
                .total(new BigDecimal(Integer.valueOf(
                        dataMap.get("total_amount"))))
                .trxTime(dateTime)
                .category(EnumTypes.CatType.OTHER)// @TODO determine the category by supplier_name
                .platform(EnumTypes.PltfmType.OTHER)// @TODO determine the platform by supplier_address
                .paymentType(getPaymentType(dataMap.get("payment_type")))
                .merchant(dataMap.get("supplier_name"))
                .build();
    }

    //to normalize the payment type
    private EnumTypes.PmtsType getPaymentType(String paymentType) {
        switch (paymentType.toLowerCase()) {
            case "cash": 
                return EnumTypes.PmtsType.CASH;
            case "visa": 
                return EnumTypes.PmtsType.CREDIT;
            default: 
                return EnumTypes.PmtsType.OTHER;
        }
    }
    

    //To formats date 09/06/24(DD/MM/YY),  2024-06-23, July 10, 2024,  14 Jun 24 18:21 +0800, 
    //and purchase_time: 18:21 +0800, 1:05,14:51, 11:20:30
    private String formatDateTIme(String dateStr, String timeStr) {
        //dateTime.replace("T", " ");
        LocalDateTime dateTime = DateTimeParser.parseDateTime(dateStr, timeStr);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
        String formattedDateTime = dateTime.format(formatter);
        System.out.println("Formatted LocalDateTime: " + formattedDateTime);
        return formattedDateTime;
    }


}
