package com.trading.journal.authentication.api;

import com.trading.journal.authentication.jwt.data.AccessTokenInfo;
import com.trading.journal.authentication.registration.UserRegistration;
import com.trading.journal.authentication.tenancy.Tenancy;
import com.trading.journal.authentication.user.AuthoritiesChange;
import com.trading.journal.authentication.user.UserInfo;
import com.trading.journal.authentication.userauthority.UserAuthority;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Api(tags = "Organisation Users Api")
@RequestMapping("/organisation/tenancy")
public interface OrganisationTenancyApi {

    @ApiOperation(notes = "Get by id", value = "Get record by its id")
    @ApiResponses(@ApiResponse(code = 200, message = "Record retrieved"))
    @GetMapping("/{id}")
    @ResponseStatus(HttpStatus.OK)
    ResponseEntity<Tenancy> getById(AccessTokenInfo accessTokenInfo, @PathVariable Long id);
}