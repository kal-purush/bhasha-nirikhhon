
package acme.features.any;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import acme.entities.banner.Banner;
import acme.framework.components.accounts.Any;
import acme.framework.controllers.AbstractController;

@Controller
public class AnyPrincipalBannerController extends AbstractController<Any, Banner> {

	@Autowired
	protected AnyPrincipalBannerListActiveService	listService;

	@Autowired
	protected AnyPrincipalBannerShowService			showService;


	@PostConstruct
	protected void initialise() {
		super.addBasicCommand("list", this.listService);
		super.addBasicCommand("show", this.showService);
	}

}