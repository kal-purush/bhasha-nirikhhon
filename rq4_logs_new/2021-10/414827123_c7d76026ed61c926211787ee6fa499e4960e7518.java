package com.bitozen.hms.rabbit.consumer.employmentletter;

import com.bitozen.hms.common.dto.GenericResponseDTO;
import com.bitozen.hms.common.dto.RabbitFileDTO;
import com.bitozen.hms.common.type.ProjectType;
import com.bitozen.hms.common.util.LogOpsUtil;
import com.bitozen.hms.pm.common.EmploymentLetterStatus;
import com.bitozen.hms.pm.repository.employmentletter.EmploymentLetterRepository;
import com.bitozen.hms.projection.employmentletter.EmploymentLetterEntryProjection;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.util.Base64;
import com.jlefebure.spring.boot.minio.MinioService;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

/**
 *
 * @author Jeremia
 */
@Service
@Slf4j
public class EmploymentLetterConsumer {

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private EmploymentLetterRepository repository;

    @Autowired
    private MinioService minioService;

    @Autowired
    RabbitTemplate rabbitTemplate;

    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = "q.employmentletter-upload-document", durable = "true"),
            exchange = @Exchange(name = "x-hms-pm", type = ExchangeTypes.TOPIC, durable = "true"),
            key = "employmentletter-upload-document",
            ignoreDeclarationExceptions = "true"), concurrency = "1")
    public void uploadImageConsumer(RabbitFileDTO dataParam) {
        try {
            byte[] decodedBytes = Base64.decodeBase64(dataParam.getImageFile());
            InputStream dataImage = new ByteArrayInputStream(decodedBytes);

            Path path = Paths.get("EMPLOYMENT_DOCUMENT_" + dataParam.getId() + "-" + dataParam.getOriginalFileName());
            Map<String, String> header = new HashMap<>();
            header.put("employmentletter", dataParam.getId());
            Optional<EmploymentLetterEntryProjection> employmentLetter = repository.findOneByElIDAndElStatusNot(dataParam.getId(), EmploymentLetterStatus.INACTIVE);
            employmentLetter.get().setElDocURL(path.toString());

            minioService.upload(path, dataImage, dataParam.getContentType(), header);
            repository.save(employmentLetter.get());
            log.info(objectMapper.writeValueAsString(LogOpsUtil.getLogResponse(
                    ProjectType.CRUD, " EmploymentLetter Upload Document", new Date(), "UPLOAD", new GenericResponseDTO().successResponse().getCode(),
                    new GenericResponseDTO().successResponse().getMessage())));
        } catch (Exception ex) {
            try {
                log.info(objectMapper.writeValueAsString(
                        LogOpsUtil.getErrorResponse(ProjectType.CRUD, " EmploymentLetter Upload Document", new Date(), "UPLOAD", String.valueOf(HttpStatus.INTERNAL_SERVER_ERROR.value()), ex.getStackTrace())));
            } catch (JsonProcessingException jpex) {
                log.info(jpex.getMessage());
            }
        }
    }

}