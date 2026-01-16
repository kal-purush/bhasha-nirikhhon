package com.hm.peopledata.Controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import com.hm.peopledata.Entity.Error;
import com.hm.peopledata.Service.ErrorService;

import java.util.Collection;

@RestController
@RequestMapping("/errors")

public class ErrorController {

    @Autowired
    private ErrorService errorService;

    @RequestMapping(method = RequestMethod.GET)
    public Collection<Error> getAllErrors(){
        return errorService.getAllErrors();
    }

    @RequestMapping(value = "/{messageid}", method = RequestMethod.GET)
    public Error getErrorByMessageId(@PathVariable("messageid") String messageid){
        return errorService.getErrorByMessageId(messageid);
    }
    
    @RequestMapping(value = "/country/{country}", method = RequestMethod.GET)
    public Collection<Error> getErrorByCountry(@PathVariable("country") String country){
        return errorService.getErrorByCountry(country);
    }
    
    @RequestMapping(value = "/solve", method = RequestMethod.GET)
    public String redirect() {
        return "Index.html";
    }

    
    @RequestMapping(value = "/{messageid}", method = RequestMethod.DELETE)
    public void deleteStudentById(@PathVariable("messageid") String messageid){
        errorService.removeErrorByMessageId(messageid);
    }

    @RequestMapping(method = RequestMethod.PUT, consumes = MediaType.APPLICATION_JSON_VALUE)
    public void deleteErrorByMessageId(@RequestBody Error error){
        errorService.updateError(error);
    }

    @RequestMapping(method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    public void insertError(@RequestBody Error error){
        errorService.insertError(error);
    }
    

}