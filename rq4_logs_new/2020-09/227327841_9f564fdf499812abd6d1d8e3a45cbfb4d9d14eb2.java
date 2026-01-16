package eu.europa.ec.fisheries.uvms.incident.service.bean;

import eu.europa.ec.fisheries.uvms.asset.client.model.CreatePollResultDto;
import eu.europa.ec.fisheries.uvms.commons.date.JsonBConfigurator;
import eu.europa.ec.fisheries.uvms.incident.model.dto.IncidentTicketDto;
import eu.europa.ec.fisheries.uvms.movementrules.model.dto.MovementDetails;
import eu.europa.ec.fisheries.uvms.rest.security.InternalRestTokenHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

@Stateless
public class AssetCommunicationBean {

    private static final Logger LOG = LoggerFactory.getLogger(AssetCommunicationBean.class);

    @Resource(name = "java:global/asset_endpoint")
    private String assetEndpoint;

    @EJB
    private InternalRestTokenHandler tokenHandler;

    public String createPollInternal(IncidentTicketDto dto) {
        try {
            String username = "Triggerd by asset not sending";
            String comment = "This poll was triggered by asset not sending on: " + Instant.now().toString() + " on Asset: " + dto.getRecipient();

            Response createdPollResponse = getWebTarget()
                    .path("internal/createPollForAsset")
                    .path(dto.getAssetId())
                    .queryParam("username", username)
                    .queryParam("comment", comment)
                    .request(MediaType.APPLICATION_JSON)
                    .header(HttpHeaders.AUTHORIZATION, tokenHandler.createAndFetchToken("user"))
                    .post(Entity.json(""), Response.class);

            if (createdPollResponse.getStatus() != 200) {
                return stripExceptionFromResponseString(createdPollResponse.readEntity(String.class));
            }

            CreatePollResultDto createPollResultDto = createdPollResponse.readEntity(CreatePollResultDto.class);
            if (!createPollResultDto.isUnsentPoll()) {
                return createPollResultDto.getSentPolls().get(0);
            } else {
                return createPollResultDto.getUnsentPolls().get(0);
            }

        } catch (Exception e) {
            LOG.error("Error while sending rule-triggered poll: ", e);
            return "NOK " + e.getMessage();
        }
    }

    private WebTarget getWebTarget() {
        Client client = ClientBuilder.newBuilder()
                .connectTimeout(30, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .build().register(JsonBConfigurator.class);
        return client.target(assetEndpoint);
    }

    private String stripExceptionFromResponseString(String errorString){
        if(!errorString.contains("Exception")){
            return errorString;
        }
        int exceptionEndIndex = errorString.indexOf("Exception:") + 10;
        return errorString.length() > exceptionEndIndex
                ? errorString.substring(exceptionEndIndex).trim() : "";
    }
}