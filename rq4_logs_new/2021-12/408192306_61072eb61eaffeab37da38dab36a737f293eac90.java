package com.wellness.tracking.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.wellness.tracking.dto.ListingDTO;
import com.wellness.tracking.enums.ListingType;
import com.wellness.tracking.model.Event;
import com.wellness.tracking.model.MediaContent;
import com.wellness.tracking.model.PublicUser;
import com.wellness.tracking.model.Tag;
import lombok.Getter;
import lombok.Setter;

import javax.validation.constraints.NotNull;
import java.sql.Date;
import java.util.Collection;
import java.util.List;

@Getter
@Setter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class EnrollmentDTO<PK extends java.io.Serializable> {
    private PK id;

    @NotNull
    private Date startDate;

    @NotNull
    private ListingDTO listing;
}