
package acme.features.any.peeps;

import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import acme.entities.peep.Peep;
import acme.framework.components.accounts.Any;
import acme.framework.components.accounts.Principal;
import acme.framework.components.accounts.UserAccount;
import acme.framework.components.models.Tuple;
import acme.framework.helpers.MomentHelper;
import acme.framework.services.AbstractService;

@Service
public class AnyPeepCreateService extends AbstractService<Any, Peep> {

	// Internal state ---------------------------------------------------------

	@Autowired
	protected AnyPeepRepository repository;

	// AbstractService interface ----------------------------------------------


	@Override
	public void check() {
		super.getResponse().setChecked(true);
	}

	@Override
	public void authorise() {
		super.getResponse().setAuthorised(true);
	}

	@Override
	public void load() {
		final Peep object = new Peep();
		String nick;
		UserAccount userAccount;

		final Date moment = MomentHelper.getCurrentMoment();
		final Principal principal = super.getRequest().getPrincipal();
		if (principal.isAnonymous())
			nick = "";
		else {
			userAccount = this.repository.findOneUserAccountById(principal.getAccountId());
			nick = userAccount.getIdentity().getName() + " " + userAccount.getIdentity().getSurname();
		}

		object.setNick(nick);
		object.setMoment(moment);

		super.getBuffer().setData(object);
	}

	@Override
	public void bind(final Peep object) {
		assert object != null;

		super.bind(object, "moment", "title", "nick", "message", "email", "link");

	}

	@Override
	public void validate(final Peep object) {
		assert object != null;
	}

	@Override
	public void perform(final Peep object) {
		assert object != null;
		final Date moment = MomentHelper.getCurrentMoment();
		object.setMoment(moment);
		this.repository.save(object);
	}

	@Override
	public void unbind(final Peep object) {
		assert object != null;

		Tuple tuple;

		tuple = super.unbind(object, "moment", "title", "nick", "message", "email", "link");

		super.getResponse().setData(tuple);

	}

}