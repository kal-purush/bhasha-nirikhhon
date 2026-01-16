
package gt.edu.usac.cunoc.ingenieria.eps.project.view;

import gt.edu.usac.cunoc.ingenieria.eps.project.Project;
import gt.edu.usac.cunoc.ingenieria.eps.project.facade.ProjectFacadeLocal;
import java.io.Serializable;
import javax.annotation.PostConstruct;
import javax.faces.view.ViewScoped;
import javax.inject.Named;
import javax.ejb.EJB;
import org.primefaces.event.FileUploadEvent;
import org.primefaces.model.UploadedFile;

@Named
@ViewScoped
public class CreateProjectView implements Serializable{
    
    @EJB
    private ProjectFacadeLocal projectFacade;
    
    private UploadedFile schedule;
    private UploadedFile investmentPlan;
    private UploadedFile annexed;

    private Project project;

    @PostConstruct
    public void init() {

    }
    
    public Project getProject() {
        if (project == null){
            return new Project();
        }
        return project;
    }

    public void handleSchedule(FileUploadEvent event){
        schedule = event.getFile();
    }
    
    public void handleInvestmentPlan(FileUploadEvent event){
        investmentPlan = event.getFile();
    }
    
    public void handleAnnexed(FileUploadEvent event){
        annexed = event.getFile();
    }
  
    public Boolean nullFiles() {
        if (schedule == null || investmentPlan == null) {
            MessageUtils.addErrorMessage("Ingrese los documentos obligatorios *");
            return true;
        }
        return false;
    }
}