package br.com.codenation.v1.errorManager.service.interfaces;

import br.com.codenation.v1.errorManager.dto.ApplicationDTO;
import br.com.codenation.v1.errorManager.dto.ApplicationInfoDTO;
import br.com.codenation.v1.errorManager.entity.Application;

import java.util.List;

public interface ApplicationServiceInterface extends ServiceInterface<Application>{

    List<ApplicationInfoDTO> findApplications(ApplicationInfoDTO filtro);

    List<ApplicationInfoDTO> findByUserId();

    Application saveApplication(ApplicationDTO name);

    void deleteApplication(Long id);

}