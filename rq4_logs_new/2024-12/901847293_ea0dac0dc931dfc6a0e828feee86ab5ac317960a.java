package com.tinqin.academy.reporting.apiadapter;

import com.tinqin.academy.reporting.api.models.ApiError;
import com.tinqin.academy.reporting.api.operations.createrecord.CreateRecordInput;
import com.tinqin.academy.reporting.api.operations.createrecord.CreateRecordOutput;
import com.tinqin.academy.reporting.api.operations.getrecord.GetRecordInput;
import com.tinqin.academy.reporting.api.operations.getrecord.GetRecordOutput;
import com.tinqin.academy.reporting.api.operations.postevent.CreateEventInput;
import com.tinqin.academy.reporting.api.operations.postevent.CreateEventOutput;
import com.tinqin.academy.reporting.apiadapter.errors.OperationError;
import com.tinqin.academy.reporting.apiadapter.mappers.CreateEventMapper;
import com.tinqin.academy.reporting.apiadapter.mappers.CreateRecordMapper;
import com.tinqin.academy.reporting.apiadapter.mappers.GetRecordMapper;
import com.tinqin.academy.reporting.apiadapter.mappers.ModelMapper;
import com.tinqin.academy.reporting.apiadapter.operations.createrecord.CreateRecord;
import com.tinqin.academy.reporting.apiadapter.operations.createrecord.ReportingCreateRecordInput;
import com.tinqin.academy.reporting.apiadapter.operations.createrecord.ReportingCreateRecordOutput;
import com.tinqin.academy.reporting.apiadapter.operations.getrecord.GetRecord;
import com.tinqin.academy.reporting.apiadapter.operations.getrecord.ReportingGetRecordInput;
import com.tinqin.academy.reporting.apiadapter.operations.getrecord.ReportingGetRecordOutput;
import com.tinqin.academy.reporting.apiadapter.operations.postevent.CreateEvent;
import com.tinqin.academy.reporting.apiadapter.operations.postevent.ReportingCreateEventInput;
import com.tinqin.academy.reporting.apiadapter.operations.postevent.ReportingCreateEventOutput;
import io.vavr.control.Either;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ApiAdapter {
    //Mappers
    private final CreateEventMapper createEventMapper;
    private final CreateRecordMapper createRecordMapper;
    private final GetRecordMapper getRecordMapper;
    private final ModelMapper modelMapper;

    //Processors
    private final CreateEvent createEvent;
    private final CreateRecord createRecord;
    private final GetRecord getRecord;

    public Either<ApiError, CreateRecordOutput> createRecord(CreateRecordInput apiInput) {
        ReportingCreateRecordInput reportingInput = createRecordMapper.toReporting(apiInput);

        Either<OperationError, ReportingCreateRecordOutput> processed = createRecord.process(reportingInput);

        return processed.isRight()
                ? Either.right(createRecordMapper.toApiResult(processed.get()))
                : Either.left(modelMapper.toApiError(processed.getLeft()));
    }

    public Either<ApiError, GetRecordOutput> getRecord(GetRecordInput apiInput) {
        ReportingGetRecordInput reportingInput = getRecordMapper.toReporting(apiInput);

        Either<OperationError, ReportingGetRecordOutput> processed = getRecord.process(reportingInput);

        return processed.isRight()
                ? Either.right(getRecordMapper.toApiResult(processed.get()))
                : Either.left(modelMapper.toApiError(processed.getLeft()));
    }

    public Either<ApiError, CreateEventOutput> createEvent(CreateEventInput apiInput) {
        ReportingCreateEventInput reportingInput = createEventMapper.toReporting(apiInput);

        Either<OperationError, ReportingCreateEventOutput> processed = createEvent.process(reportingInput);

        return processed.isRight()
                ? Either.right(createEventMapper.toApiResult(processed.get()))
                : Either.left(modelMapper.toApiError(processed.getLeft()));
    }
}