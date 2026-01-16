package uk.gov.hmcts.reform.lrdapi.controllers;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import uk.gov.hmcts.reform.lrdapi.controllers.response.LrdCourtVenuesByServiceCodeResponse;
import uk.gov.hmcts.reform.lrdapi.service.CourtVenueService;

import javax.validation.constraints.NotBlank;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;
import static uk.gov.hmcts.reform.lrdapi.service.impl.CourtVenueServiceImpl.validateServiceCode;

@RequestMapping(
    path = "/refdata/location/court-venue"
)
@RestController
@Slf4j
public class LrdCourtVenueController {

    @Autowired
    CourtVenueService courtVenueService;

    @ApiOperation(
        value = "This API will retrieve Court Venues for given Service Code",
        notes = "No roles required to access this API",
        authorizations = {
            @Authorization(value = "ServiceAuthorization"),
            @Authorization(value = "Authorization")
        }
    )
    @ApiResponses({
        @ApiResponse(
            code = 200,
            message = "Successfully retrieved list of Court Venues for given Service Code",
            response = LrdCourtVenuesByServiceCodeResponse.class
        ),
        @ApiResponse(
            code = 400,
            message = "Bad Request"
        ),
        @ApiResponse(
            code = 401,
            message = "Forbidden Error: Access denied"
        ),
        @ApiResponse(
            code = 404,
            message = "No Court Venues found with the given Service Code"
        ),
        @ApiResponse(
            code = 500,
            message = "Internal Server Error"
        )
    })
    @GetMapping(
        path = "/services",
        produces = APPLICATION_JSON_VALUE
    )
    public ResponseEntity<Object> retrieveCourtVenuesByServiceCode(
        @RequestParam(value = "service_code", required = true) @NotBlank String serviceCode) {

        validateServiceCode(serviceCode.trim());

        LrdCourtVenuesByServiceCodeResponse response = courtVenueService
            .retrieveCourtVenuesByServiceCode(serviceCode.trim());

        return ResponseEntity.status(HttpStatus.OK).body(response);
    }
}