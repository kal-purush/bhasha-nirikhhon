package com.tinqin.library.reporting.apiadapter.mappers;

import com.tinqin.library.reporting.api.models.ApiError;
import com.tinqin.library.reporting.api.models.Event;
import com.tinqin.library.reporting.api.models.Record;
import com.tinqin.library.reporting.apiadapter.errors.OperationError;
import com.tinqin.library.reporting.apiadapter.models.ReportingEvent;
import com.tinqin.library.reporting.apiadapter.models.ReportingRecord;
import org.mapstruct.Mapper;
import org.mapstruct.factory.Mappers;

@Mapper(componentModel = "spring")
public interface ModelMapper {

    CreateRecordMapper INSTANCE = Mappers.getMapper(CreateRecordMapper.class);

    ApiError toApiError(OperationError operationError);

    Event toEvent(ReportingEvent reportingEvent);

    Record toRecord(ReportingRecord reportingRecord);
}