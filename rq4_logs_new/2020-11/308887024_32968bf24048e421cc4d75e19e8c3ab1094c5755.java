package com.marketplace.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marketplace.domain.classifiedad.ClassifiedAd;
import com.marketplace.domain.classifiedad.ClassifiedAdId;
import com.marketplace.domain.classifiedad.CommandHandlerResult;
import com.marketplace.domain.classifiedad.command.CreateClassifiedAd;
import com.marketplace.domain.classifiedad.command.UpdateClassifiedAd;
import com.marketplace.domain.classifiedad.controller.ClassifiedAdController;
import com.marketplace.domain.classifiedad.controller.CreateAdResponse;
import org.eclipse.jetty.http.HttpStatus;
import spark.Route;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.marketplace.server.SparkServer.MEDIA_APPLICATION_JSON;

@Singleton
@Named
public class ClassifiedAdSparkRoutes {
    public static final String HEADER_CONTENT_TYPE = "Content-Type";
    private final ObjectMapper objectMapper;
    private final ClassifiedAdController classifiedAdController;

    @Inject
    public ClassifiedAdSparkRoutes(ObjectMapper objectMapper, ClassifiedAdController classifiedAdController) {
        this.objectMapper = objectMapper;
        this.classifiedAdController = classifiedAdController;
    }

    public Route createClassifiedAdRoute() {
        return (request, response) -> {
            byte[] body = request.bodyAsBytes();
            CreateClassifiedAd createAdDto = objectMapper.readValue(body, CreateClassifiedAd.class);
            CommandHandlerResult<CreateAdResponse> ad = classifiedAdController.createAd(createAdDto);
            if (ad.isSuccessful()) {
                var createAdResponse = ad.result;
                response.header(HEADER_CONTENT_TYPE, MEDIA_APPLICATION_JSON);
                response.type(MEDIA_APPLICATION_JSON);
                response.status(201);
                response.header("Location", String.format("/classified_ad/%s", createAdResponse.getId().toString()));
                return null;
            } else {
                Map<String, Object> resMessage = Map.of(
                        "status", ad.isSuccessful(),
                        "message", ad.getMessage()
                );
                String s = objectMapper.writeValueAsString(resMessage);
                response.status(404);
                return s;
            }
        };
    }

    public Route updateClassifiedAd() {
        return (request, response) -> {
            String classifiedAdId = request.params(":classifiedAdId");
            response.type("application/json");
            try {
                byte[] body = request.bodyAsBytes();
                var updateDto = objectMapper.readValue(body, UpdateClassifiedAd.class);
                updateDto.setId(UUID.fromString(classifiedAdId));
                var commandResult = classifiedAdController.updateClassifiedAd(updateDto);
                if (commandResult.isSuccessful()) {
                    response.status(204);
                } else {
                    Map<String, Object> resMessage = Map.of(
                            "status", commandResult.isSuccessful(),
                            "message", commandResult.getMessage()
                    );
                    String s = objectMapper.writeValueAsString(resMessage);
                    response.status(404);
                    return s;
                }
            } catch (JsonMappingException ex) {
                response.status(502);
            }
            return null;
        };
    }

    public Route findClassifiedAdById() {
        return (request, response) -> {
            String classifiedAdId = request.params(":classifiedAdId");
            Optional<ClassifiedAd> mayBe = classifiedAdController
                    .findClassifiedAdById(ClassifiedAdId.fromString(classifiedAdId));
            return mayBe.map(classifiedAd -> {
                String result = null;
                try {
                    result = objectMapper.writeValueAsString(classifiedAd);
                    response.status(200);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
                return result;
            }).orElseGet(() -> {
                response.status(404);
                return null;
            });
        };
    }

    public Route updateClassifiedAdTitle() {
        return (req, res) -> {
            String classifiedAdId = req.params(":classifiedAdId");
            Optional<ClassifiedAd> mayBe = classifiedAdController
                    .findClassifiedAdById(ClassifiedAdId.fromString(classifiedAdId));
            return mayBe.map(classifiedAd -> {
                String result = null;
                try {
                    result = objectMapper.writeValueAsString(classifiedAd);
                    res.status(200);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
                return result;
            }).orElseGet(() -> {
                res.status(404);
                return null;
            });
        };
    }

    public Route updateClassifiedAdOwner() {
        return (req, res) -> {
            return null;
        };
    }

    public Route updateClassifiedAdText() {
        return (req, res) -> {
            return null;
        };
    }

    public Route approveClassifiedAd() {
        return (req, res) -> {
            return null;
        };
    }

    public Route publishClassifiedAd() {
        return (req, res) -> {
            return null;
        };
    }
}