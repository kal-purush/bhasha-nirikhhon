package service;

import model.fildClass.ApplicationName;
import model.fildClass.ComputerName;
import org.springframework.stereotype.Service;
import repository.ApplicationNameRepository;

@Service
public class ApplicationNameService {

    private final ApplicationNameRepository applicationNameRepository;

    public ApplicationNameService(ApplicationNameRepository applicationNameRepository) {
        this.applicationNameRepository = applicationNameRepository;
    }

    ApplicationName findByName(String s){
        return applicationNameRepository.findByName(s);
    }

    public ApplicationName save(ApplicationName applicationName){
        return applicationNameRepository.saveAndFlush(applicationName);
    }

}