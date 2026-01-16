package eu.europa.ec.fisheries.uvms.incident.service.bean;

import eu.europa.ec.fisheries.uvms.commons.date.JsonBConfigurator;
import eu.europa.ec.fisheries.uvms.incident.model.dto.enums.RiskLevel;
import eu.europa.ec.fisheries.uvms.incident.service.domain.entities.Incident;
import eu.europa.ec.fisheries.uvms.movement.client.MovementRestClient;
import eu.europa.ec.fisheries.uvms.movement.client.model.MicroMovement;
import eu.europa.ec.fisheries.uvms.spatial.model.schemas.AreaByLocationSpatialRQ;
import eu.europa.ec.fisheries.uvms.spatial.model.schemas.AreaExtendedIdentifierType;
import eu.europa.ec.fisheries.uvms.spatial.model.schemas.AreaType;
import eu.europa.ec.fisheries.uvms.spatial.model.schemas.PointType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.ejb.Stateless;
import javax.inject.Inject;
import javax.json.bind.Jsonb;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Stateless
public class RiskCalculationsBean {

    private static final Logger LOG = LoggerFactory.getLogger(RiskCalculationsBean.class);

    private WebTarget webTarget;

    @Resource(name = "java:global/spatial_endpoint")
    private String spatialEndpoint;

    private Jsonb jsonb;

    @Inject
    private MovementRestClient movementClient;

    @PostConstruct
    public void initClient() {
        String url = spatialEndpoint + "/spatialnonsecure/json/";
        JsonBConfigurator jsonBConfigurator = new JsonBConfigurator();
        jsonb = jsonBConfigurator.getContext(null);

        ClientBuilder clientBuilder = ClientBuilder.newBuilder();
        clientBuilder.connectTimeout(30, TimeUnit.SECONDS);
        clientBuilder.readTimeout(30, TimeUnit.SECONDS);
        Client client = clientBuilder.build();
        client.register(jsonBConfigurator);
        webTarget = client.target(url);
    }


    public RiskLevel calculateRiskLevelForIncident(Incident incident){
        if(incident.getMovementId() == null){
            return null;
        }
        MicroMovement microMovement = movementClient.getMicroMovementById(incident.getMovementId());
        if(checkIfPositionIsInPortArea(microMovement)){
            return RiskLevel.LOW;
        } else {
            return RiskLevel.HIGH;
        }
    }

    public boolean checkIfPositionIsInPortArea(MicroMovement microMovement){
        AreaByLocationSpatialRQ request = new AreaByLocationSpatialRQ();
        request.setPoint(new PointType(microMovement.getLocation().getLongitude(), microMovement.getLocation().getLatitude(), 4326));

        String response =  webTarget
                .path("getAreaByLocation")
                .request(MediaType.APPLICATION_JSON)
                .post(Entity.json(request), String.class);
        List<AreaExtendedIdentifierType> areaList = jsonb.fromJson(response, new ArrayList<AreaExtendedIdentifierType>() {}.getClass().getGenericSuperclass());

        Optional<AreaExtendedIdentifierType> portArea = areaList.stream().filter(a -> a.getAreaType().equals(AreaType.PORTAREA)).findAny();

        if(portArea.isPresent()){
            return true;
        }
        return false;
    }
}