package acme.features.authenticated.bulletin;

import acme.entities.bulletin.Bulletin;
import acme.framework.components.accounts.Authenticated;
import acme.framework.controllers.AbstractController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import javax.annotation.PostConstruct;

@Controller
public class AuthenticatedBulletinController extends AbstractController<Authenticated, Bulletin> {

    // Internal state ---------------------------------------------------------

    @Autowired
    protected AuthenticatedBulletinListService listService;
    @Autowired
    protected AuthenticatedBulletinShowService showService;

    // Constructors -----------------------------------------------------------

    @PostConstruct
    protected void initialise() {
        super.addBasicCommand("list", this.listService);
        super.addBasicCommand("show", this.showService);
    }



}