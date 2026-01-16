package cz.martlin.jrest.impl.jarmil.handler;

import cz.martlin.jrest.impl.jarmil.misc.JarmilEnvironment;
import cz.martlin.jrest.impl.jarmil.reqresp.JarmilRequest;
import cz.martlin.jrest.impl.jarmil.reqresp.JarmilResponse;
import cz.martlin.jrest.impl.jarmil.targets.waiter.ObjectOnWaiterTarget;
import cz.martlin.jrest.waiter.JRestWaiter;
import cz.martlin.jrest.waiter.RequestHandler;

/**
 * The handler of Jarmil requests.
 * 
 * @author martin
 *
 */
public class JarmilHandler implements RequestHandler<JarmilRequest, JarmilResponse> {

	private final JarmilRequestsPerformer performer;
	private WaiterStopper stopper;

	public JarmilHandler(JarmilEnvironment environment) {
		super();

		JarmilEnvironment env = prepare(environment);
		this.performer = new JarmilRequestsPerformer(env);
	}

	@Override
	public void initialize(JRestWaiter<JarmilRequest, JarmilResponse> waiter) throws Exception {
		stopper.setWaiter(waiter);
	}

	@Override
	public void finish(JRestWaiter<JarmilRequest, JarmilResponse> waiter) throws Exception {
	}

	@Override
	public JarmilResponse handle(JarmilRequest request) throws Exception {
		return performer.perform(request);
	}

	/**
	 * Updates given environment such that adds a {@link WaiterStopper} and
	 * {@link Echoer}. The stopper is also set in the {@link #stopper} slot to
	 * be nextly initialized by {@link #initialize(JRestWaiter)} method.
	 * 
	 * @param environment
	 * @return
	 */
	private JarmilEnvironment prepare(JarmilEnvironment environment) {
		stopper = new WaiterStopper();
		ObjectOnWaiterTarget stopperTarget = ObjectOnWaiterTarget.create(WaiterStopper.OBJECT_NAME, stopper);
		environment.add(stopperTarget);

		Echoer echoer = new Echoer();
		ObjectOnWaiterTarget echoerTarget = ObjectOnWaiterTarget.create(Echoer.OBJECT_NAME, echoer);
		environment.add(echoerTarget);
		return environment;
	}

}