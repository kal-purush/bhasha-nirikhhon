package org.mlsk.service.impl.classifier.decisiontree.api;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mlsk.api.classifier.model.ClassifierDataRequestModel;
import org.mlsk.api.classifier.model.ClassifierDataResponseModel;
import org.mlsk.api.classifier.model.ClassifierStartRequestModel;
import org.mlsk.api.classifier.model.ClassifierStartResponseModel;
import org.mlsk.api.decisiontree.api.DecisionTreeApi;
import org.mlsk.service.model.classifier.ClassifierDataResponse;
import org.mlsk.service.model.classifier.ClassifierStartResponse;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;

import static com.google.common.collect.Lists.newArrayList;
import static org.mlsk.service.impl.classifier.mapper.ClassifierDataResponseMapper.toClassifierDataResponseModel;
import static org.mlsk.service.impl.classifier.mapper.ClassifierStartResponseMapper.toClassifierStartResponseModel;

@RestController
public class DecisionTreeApiImpl implements DecisionTreeApi {

  private static final Logger LOGGER = LogManager.getLogger(DecisionTreeApiImpl.class);

  @Override
  public ResponseEntity<ClassifierStartResponseModel> start(ClassifierStartRequestModel classifierStartRequestModel) {
    LOGGER.info("Start request received");
    return ResponseEntity.ok(toClassifierStartResponseModel(new ClassifierStartResponse("id1")));
  }

  @Override
  public ResponseEntity<Void> data(ClassifierDataRequestModel classifierDataRequestModel) {
    LOGGER.info("Data request received with request id {}", classifierDataRequestModel.getRequestId());
    return ResponseEntity.ok().build();
  }

  @Override
  public ResponseEntity<ClassifierDataResponseModel> predict() {
    LOGGER.info("Predict request received");
    return ResponseEntity.ok(toClassifierDataResponseModel(new ClassifierDataResponse("col1", newArrayList(0, 1, 1))));
  }

  @Override
  public ResponseEntity<BigDecimal> computePredictAccuracy() {
    LOGGER.info("Compute predict Accuracy request received");
    return ResponseEntity.ok(BigDecimal.ONE);
  }
}