package com.tinqin.library.reporting.apiadapter.mappers;

import com.tinqin.library.reporting.api.operations.closerecord.CloseRecordInput;
import com.tinqin.library.reporting.api.operations.closerecord.CloseRecordOutput;
import com.tinqin.library.reporting.apiadapter.operations.closerecord.ReportingCloseRecordInput;
import com.tinqin.library.reporting.apiadapter.operations.closerecord.ReportingCloseRecordOutput;
import org.mapstruct.Mapper;
import org.mapstruct.factory.Mappers;

@Mapper(componentModel = "spring", uses = ModelMapper.class)
public interface CloseRecordMapper {

    CloseRecordMapper INSTANCE = Mappers.getMapper(CloseRecordMapper.class);

    ReportingCloseRecordInput toReporting(CloseRecordInput closeRecordInput);

    CloseRecordOutput toApiResult(ReportingCloseRecordOutput output);
}