package eu.europa.ec.fisheries.uvms.incident.model.dto;

import eu.europa.ec.fisheries.uvms.incident.model.dto.enums.MovementSourceType;

import java.time.Instant;
import java.util.UUID;


public class MovementDto {

    private UUID id;

    private MovementPointDto location;

    private Float speed;

    private Float heading;

    private Double tripNumber;

    private String asset;

    private String internalReferenceNumber;

    private String status;

    private MovementSourceType source;

    private String movementType;

    private Instant timestamp;

    private Instant lesReportTime;

    private String sourceSatelliteId;

    private Instant updated;

    private String updatedBy;

    //Value can be 0 (>10m) and 1 (<10m). See https://gpsd.gitlab.io/gpsd/AIVDM.html for more info
    private Short aisPositionAccuracy;

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public MovementPointDto getLocation() {
        return location;
    }

    public void setLocation(MovementPointDto location) {
        this.location = location;
    }

    public Float getSpeed() {
        return speed;
    }

    public void setSpeed(Float speed) {
        this.speed = speed;
    }

    public Float getHeading() {
        return heading;
    }

    public void setHeading(Float heading) {
        this.heading = heading;
    }

    public Double getTripNumber() {
        return tripNumber;
    }

    public void setTripNumber(Double tripNumber) {
        this.tripNumber = tripNumber;
    }

    public String getAsset() {
        return asset;
    }

    public void setAsset(String asset) {
        this.asset = asset;
    }

    public String getInternalReferenceNumber() {
        return internalReferenceNumber;
    }

    public void setInternalReferenceNumber(String internalReferenceNumber) {
        this.internalReferenceNumber = internalReferenceNumber;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public MovementSourceType getSource() {
        return source;
    }

    public void setSource(MovementSourceType source) {
        this.source = source;
    }

    public String getMovementType() {
        return movementType;
    }

    public void setMovementType(String movementType) {
        this.movementType = movementType;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public Instant getLesReportTime() {
        return lesReportTime;
    }

    public void setLesReportTime(Instant lesReportTime) {
        this.lesReportTime = lesReportTime;
    }

    public String getSourceSatelliteId() {
        return sourceSatelliteId;
    }

    public void setSourceSatelliteId(String sourceSatelliteId) {
        this.sourceSatelliteId = sourceSatelliteId;
    }

    public Instant getUpdated() {
        return updated;
    }

    public void setUpdated(Instant updated) {
        this.updated = updated;
    }

    public String getUpdatedBy() {
        return updatedBy;
    }

    public void setUpdatedBy(String updatedBy) {
        this.updatedBy = updatedBy;
    }

    public Short getAisPositionAccuracy() {
        return aisPositionAccuracy;
    }

    public void setAisPositionAccuracy(Short aisPositionAccuracy) {
        this.aisPositionAccuracy = aisPositionAccuracy;
    }
}