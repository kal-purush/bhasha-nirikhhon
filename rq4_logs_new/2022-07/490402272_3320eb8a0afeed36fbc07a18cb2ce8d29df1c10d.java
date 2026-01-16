package com.trading.journal.authentication.api;

import com.trading.journal.authentication.pageable.PageResponse;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;

public interface PageableApi<T> {

    @ApiOperation(notes = "Get all", value = "Get all")
    @ApiResponses(@ApiResponse(code = 200, message = "Records retrieved"))
    @GetMapping
    @ResponseStatus(HttpStatus.OK)
    ResponseEntity<PageResponse<T>> getAll(
            @RequestParam(value = "page", defaultValue = "0", required = false) Integer page,
            @RequestParam(value = "size", defaultValue = "10", required = false) Integer size,
            @ApiParam(name = "sort", value = "A array with property and direction such as \"id,asc\", \"name,desc\"") @RequestParam(value = "sort", required = false) String[] sort,
            @RequestParam(value = "filter", required = false) String filter);

    @ApiOperation(notes = "Get by id", value = "Get record by its id")
    @ApiResponses(@ApiResponse(code = 200, message = "Record retrieved"))
    @GetMapping("/{id}")
    @ResponseStatus(HttpStatus.OK)
    ResponseEntity<T> getById(@PathVariable Long id);
}