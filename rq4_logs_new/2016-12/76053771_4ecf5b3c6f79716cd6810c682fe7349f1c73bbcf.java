package project.controller;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import project.model.Owner;
import project.model.OwnerRepository;
import project.service.OwnerService;

import javax.validation.Valid;


/**
 * Created by Наиль on 23.11.2016.
 */
@Controller
public class OwnerController {
    private static final Logger log = Logger.getLogger(OwnerController.class);
    private OwnerRepository ownerRepository;
    private OwnerService ownerService;

    @Autowired
    public OwnerController(OwnerRepository ownerRepository, OwnerService ownerService){
        this.ownerRepository = ownerRepository;
        this.ownerService = ownerService;
    }

    @RequestMapping(path = "/create-owner", method = RequestMethod.POST)  //POST GET
    @ResponseBody
    //(requestbody)
    //response eintity
    public void saveOwner(@Valid @RequestBody Owner owner){
        try{
            ownerService.createOwner(owner.getFirstName(), owner.getLastName(), owner.getAddress(), owner.getPhoneNumber());
            log.info("User Created");
        }catch(Exception e){
            System.out.println(e.toString());
            log.info("Error while creating | " + e.toString());
        }
    }

    @RequestMapping(path = "/delete-owner", method = RequestMethod.DELETE)
    @ResponseBody
    public void deleteOwner(@Valid @RequestBody Long id){
        try{
            ownerService.removeOwner(id);
            log.info("Owner with id = " + id + "deleted");
        }catch(Exception e){
            System.out.println(e.toString());
            log.info("Error while deleting | " + e.toString());
        }
    }

    @RequestMapping(path = "/update-owner", method = RequestMethod.PUT)
    @ResponseBody
    public void updateOwner(@Valid @RequestBody Owner owner){
        try{
            ownerService.updateOwner(owner.getId(), owner.getFirstName(), owner.getLastName(), owner.getAddress(), owner.getPhoneNumber());
            log.info("Owner with id = " + owner.getId() + "updated");
        }catch(Exception e){
            System.out.println(e.toString());
            log.info("Error while updating | " + e.toString());
        }
    }

    @RequestMapping("/find-by-phone")
    @ResponseBody
    public String getByPhone(String phone){
        String result = null;
        try{
            Owner owner = ownerRepository.findByPhoneNumber(phone);
            result = owner.toString();
            log.info("Owner with phone = (" + phone + ")" + " extracted");
        }catch(Exception e){
            System.out.println(e.toString());
            log.info("Error while extracting " + e.toString());
        }
        return result;
    }

    @RequestMapping("/find-by-address")
    @ResponseBody
    public String getByAdress(String address){
        String result = null;
        try{
            Owner owner = ownerRepository.findByAddress(address);
            result = owner.toString();
            log.info("Owner with address = (" + address + ")" + " extracted");
        }catch(Exception e){
            System.out.println(e.toString());
            log.info("Error while extracting " + e.toString());
        }
        return result;
    }

}