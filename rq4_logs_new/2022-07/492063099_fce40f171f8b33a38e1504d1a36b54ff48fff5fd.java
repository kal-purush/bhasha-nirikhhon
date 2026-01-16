package ma.nttsquad.nttecomcore.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ma.nttsquad.nttecomcore.dto.AddressDto;
import ma.nttsquad.nttecomcore.service.AddressSrv;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Slf4j
@RequiredArgsConstructor
@RestController
@RequestMapping("/addresses")
@Tag(name = "Address", description = "The Address API")
public class AddressCtrl {

    final AddressSrv addressSrv;

    @Operation(summary = "Find all addresses", description = "Find all addresses", tags = "Address")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "successful operation", content = @Content(array = @ArraySchema(schema = @Schema(implementation = AddressDto.class)))),
            @ApiResponse(responseCode = "400", description = "Bad request", content = @Content(schema = @Schema(description = "HTTP status error code", example = "400")))
    }
    )
    @GetMapping("/all")
    public List<AddressDto> getAll(){
        return addressSrv.getAllAddresses();
    }

    @Operation(summary = "Find address by id", description = "Find Address By Id", tags = "Address")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "successful operation", content = @Content(array = @ArraySchema(schema = @Schema(implementation = AddressDto.class)))),
            @ApiResponse(responseCode = "400", description = "Bad request", content = @Content(schema = @Schema(description = "HTTP status error code", example = "400")))
    }
    )
    @GetMapping("/get/{address_id}")
    public AddressDto getAddressById(@PathVariable(name = "address_id") Long address_id) {return addressSrv.getAddressById(address_id);}

    @Operation(summary = "add new address", description = "Add New Address", tags = "Address")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "successful operation", content = @Content(array = @ArraySchema(schema = @Schema(implementation = AddressDto.class)))),
            @ApiResponse(responseCode = "400", description = "Bad request", content = @Content(schema = @Schema(description = "HTTP status error code", example = "400")))
    }
    )
    @PostMapping("/save")
    public void saveAddress(@RequestBody AddressDto addressDto){
        log.trace("{}", addressDto);
        addressSrv.saveAddress(addressDto);
    }

    @Operation(summary = "update address", description = "Update Address", tags = "Address")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "successful operation", content = @Content(array = @ArraySchema(schema = @Schema(implementation = AddressDto.class)))),
            @ApiResponse(responseCode = "400", description = "Bad request", content = @Content(schema = @Schema(description = "HTTP status error code", example = "400")))
    }
    )
    @PostMapping("/edit/{address_id}")
    public void updateAddress(@PathVariable(name = "address_id") Long address_id,@RequestBody AddressDto addressDto){
        log.trace("{}", addressDto);
        addressSrv.updateAddress(address_id,addressDto);
    }

    @Operation(summary = "delete address", description = "delete Address", tags = "Address")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "successful operation", content = @Content(array = @ArraySchema(schema = @Schema(implementation = AddressDto.class)))),
            @ApiResponse(responseCode = "400", description = "Bad request", content = @Content(schema = @Schema(description = "HTTP status error code", example = "400")))
    }
    )
    @PostMapping("/delete/{address_id}")
    public void removeAddress(@PathVariable(name = "address_id") Long address_id){
        log.trace("{}", address_id);
        addressSrv.deleteAddress(address_id);
    }
}