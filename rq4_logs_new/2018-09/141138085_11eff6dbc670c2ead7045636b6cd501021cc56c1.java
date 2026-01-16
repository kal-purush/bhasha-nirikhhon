package cms.sre.persistenceapi.controller;

import cms.sre.dna_common_data_model.emailnotifier.Email;
import cms.sre.persistenceapi.service.EmailPersistenceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
public class EmailController {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private EmailPersistenceService emailPersistenceService;

    @Autowired
    public EmailController(EmailPersistenceService emailPersistenceService){
        this.emailPersistenceService = emailPersistenceService;
    }

    @GetMapping("/email")
    public List<Email> getEmails(){ return this.emailPersistenceService.getEmails(); }

    @GetMapping("/email/{uuid}")
    public Email getProductsByTitle(@PathVariable("uuid") String uuid){

        return this.emailPersistenceService.getEmailByUuid(uuid);

    }

    @PostMapping("/email")
    public boolean saveEmail(Email email){

        this.emailPersistenceService.saveEmail(email);

        return true;
    }

    @DeleteMapping("/email/{uuid}")
    public void deleteEmail(@PathVariable String uuid){
        emailPersistenceService.deleteEmail(uuid);
    }
}