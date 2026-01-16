package com.dk.dento.care.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * Created by khana on 06/08/16.
 */
@Controller
public class ClinicController {

    @RequestMapping(
            value = "/clinics",
            method = RequestMethod.GET
    )
    @ResponseBody
    public ResponseEntity getAllClinics() {
        //id,name, get all records from clinic table.
        return null;
    }

    @RequestMapping(
            value = "/clinic",
            method = RequestMethod.POST
    )
    @ResponseBody
    public ResponseEntity createClinic() {

        //create a record in user_credentials as clinic admin.
        //create a record in user_detail.
        //create a record in clinic
        //create a record in clinic_user_mapping.
        return null;
    }

    @RequestMapping(
            value = "/clinic/{id}",
            method = RequestMethod.PUT
    )
    @ResponseBody
    public ResponseEntity updateClinic(@PathVariable final Long id) {
        return null;
    }

    @RequestMapping(
            value = "/clinic/{id}",
            method = RequestMethod.GET
    )
    @ResponseBody
    public ResponseEntity getClinic(@PathVariable final Long id) {
        return null;
    }

    @RequestMapping(
            value = "/clinic/{id}/modules",
            method = RequestMethod.GET
    )
    @ResponseBody
    public ResponseEntity getModules(@PathVariable final Long id) {
        // get all records from clinic_modules_mapping for given clinic.
        // Module (id,name)
        return null;
    }

    @RequestMapping(
            value = "/clinic/{id}/modules",
            method = RequestMethod.PUT
    )
    @ResponseBody
    public ResponseEntity saveClinicModules(@PathVariable final Long id) {
        //insert or delete records in clinic_modules_mapping for selected module ids for current clinic.
        return null;
    }

    @RequestMapping(
            value = "/clinic/{id}/users",
            method = RequestMethod.GET
    )
    @ResponseBody
    public ResponseEntity getClinicUsers(@PathVariable final Long id) {
        return null;
    }

    @RequestMapping(
            value = "/clinic/{id}/user",
            method = RequestMethod.POST
    )
    @ResponseBody
    public ResponseEntity createClinicUser(@PathVariable final Long id) {
        return null;
    }

    @RequestMapping(
            value = "/clinic/{clinicId}/user/{userId}",
            method = RequestMethod.POST
    )
    @ResponseBody
    public ResponseEntity updateClinicUser(@PathVariable final Long clinicId, @PathVariable final Long userId) {
        return null;
    }

    @RequestMapping(
            value = "/clinic/{clinicId}/user/{userId}/modules",
            method = RequestMethod.GET
    )
    @ResponseBody
    public ResponseEntity getClinicUserModuleAccess(@PathVariable final Long clinicId, @PathVariable final Long userId) {
        return null;
    }

    @RequestMapping(
            value = "/clinic/{clinicId}/user/{userId}/modules",
            method = RequestMethod.PUT
    )
    @ResponseBody
    public ResponseEntity saveClinicUserModuleAccess(@PathVariable final Long clinicId, @PathVariable final Long userId) {
        return null;
    }
}