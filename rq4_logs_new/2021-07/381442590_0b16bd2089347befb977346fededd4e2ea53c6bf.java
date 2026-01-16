package com.upgrad.FoodOrderingApp.api.controller;

import com.upgrad.FoodOrderingApp.api.model.*;
import com.upgrad.FoodOrderingApp.service.entity.AddressEntity;
import com.upgrad.FoodOrderingApp.service.entity.StateEntity;
import com.upgrad.FoodOrderingApp.service.exception.AddressNotFoundException;
import com.upgrad.FoodOrderingApp.service.exception.AuthorizationFailedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

/**
 * Created by murarka on 07/06/21.
 */

@RestController
@RequestMapping("/")

public class AddressController {

    @Autowired
    private AddressService addressService;

    @RequestMapping(method = RequestMethod.POST,
            path = "/address" ,
            consumes = MediaType.APPLICATION_JSON_UTF8_VALUE,
            produces = MediaType.APPLICATION_JSON_UTF8_VALUE)


    public ResponseEntity<SaveAddressResponse> addressSave
            (@RequestHeader("authorization") final String authorization , final SaveAddressRequest saveAddressRequest) throws AuthorizationFailedException {

        AddressEntity addressEntity = new AddressEntity();

        addressEntity.setFlatBuilNo(saveAddressRequest.getFlatBuildingName());
                //setFlatBuilNumber(saveAddressRequest.getFlatBuildingName());
        addressEntity.setLocality(saveAddressRequest.getLocality());
        addressEntity.setCity(saveAddressRequest.getCity());
        addressEntity.setPincode(saveAddressRequest.getPincode());
        addressEntity.setState(addressService.getStateByUuid(saveAddressRequest.getStateUuid()));
                //setStateEntity(addressService.getStateByUuid(saveAddressRequest.getStateUuid()));
        addressEntity.setUuid(UUID.randomUUID().toString());
        addressEntity.setActive(1);

        AddressEntity  saveAddressEntity = addressService.saveAddress(addressEntity,authorization);



        SaveAddressResponse saveAddressResponse = new SaveAddressResponse()
                .id(saveAddressEntity.getUuid())
                .status("ADDRESS SUCCESSFULLY REGISTERED");
        return new ResponseEntity<SaveAddressResponse>(saveAddressResponse, HttpStatus.CREATED);
    }

    @RequestMapping(method = RequestMethod.GET,
            path = "/address/customer" ,
            produces = MediaType.APPLICATION_JSON_UTF8_VALUE)

    public ResponseEntity<AddressListResponse>getAllPermanentAddress(
            @RequestHeader("authorization") final String authorization)throws AuthorizationFailedException
    {
        String[] bearerToken = authorization.split("Bearer ");
        List<AddressEntity> addressEntityList = addressService.getAllAddress(bearerToken[1]);

        AddressListResponse addressListResponse = new AddressListResponse();
        for (AddressEntity ae : addressEntityList) {
            StateEntity se = addressService.getStateById(ae.getState().getId());

            AddressListState addressListState = new AddressListState();

            // Sets the state to each address element
            addressListState.setStateName(se.getStateName());

            // Adds the city, flat building name, locality, pincode and state to the addressList
            AddressList addressList = new AddressList().id(UUID.fromString(ae.getUuid())).city(ae.getCity())
                    .flatBuildingName(ae.getFlatBuilNo()).locality(ae.getLocality())
                    .pincode(ae.getPincode()).state(addressListState);

            // Adds the addressList to addressListResponse
            addressListResponse.addAddressesItem(addressList);
        }

        return new ResponseEntity<AddressListResponse>(addressListResponse, HttpStatus.OK);

    }

    @RequestMapping(method = RequestMethod.DELETE,
            path = "/address/{address_id}" ,
            produces = MediaType.APPLICATION_JSON_UTF8_VALUE)

    public ResponseEntity<DeleteAddressResponse> deleteAddress(String authorization,
                                                               @PathVariable("address_id") String addressUuid)
            throws AuthorizationFailedException, AddressNotFoundException {

        String[] bearerToken = authorization.split("Bearer ");


        AddressEntity deletedAddress = addressService.deleteAddress(addressUuid, bearerToken[1]);


        DeleteAddressResponse deleteAddressResponse = new DeleteAddressResponse().id(UUID.fromString(deletedAddress.getUuid()))
                .status("ADDRESS DELETED SUCCESSFULLY");

        // Returns the DeleteAddressResponse with OK http status
        return new ResponseEntity<DeleteAddressResponse>(deleteAddressResponse, HttpStatus.OK);
    }


    @RequestMapping(method = RequestMethod.GET,
            path = "/states",
            produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity<StatesListResponse> getAllStates() throws AuthorizationFailedException {


        List<StateEntity> stateEntityList = addressService.getAllStates();
        StatesListResponse stateListResponse = new StatesListResponse();


        for (StateEntity se : stateEntityList) {
            StatesList state = new StatesList();
            state.setStateName(se.getStateName());
            state.setId(UUID.fromString(se.getStateUuid()));
            stateListResponse.addStatesItem(state);
        }

        return new ResponseEntity<StatesListResponse>(stateListResponse, HttpStatus.OK);
    }


}