package org.spiceboys.Travel.Diary.dto;

public class ItineraryDTO {
    Long itineraryId;
    Boolean isPrivate;

    public ItineraryDTO(Long id, Boolean isPrivate) {
        this.itineraryId = id;
        this.isPrivate = isPrivate;
    }

    public Long getItineraryId() {
        return itineraryId;
    }

    public void setItineraryId(Long itineraryId) {
        this.itineraryId = itineraryId;
    }

    public Boolean getPrivate() {
        return isPrivate;
    }

    public void setPrivate(Boolean aPrivate) {
        isPrivate = aPrivate;
    }
}