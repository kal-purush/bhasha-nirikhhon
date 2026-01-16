package example.tester;

import de.thorbenkuck.rhfw.annotations.AutoResolve;
import de.thorbenkuck.rhfw.annotations.RegisterModule;
import de.thorbenkuck.rhfw.interfaces.RegisterModuleInterface;

@RegisterModule
public class D implements RegisterModuleInterface {

	@AutoResolve
	public D(B b) {
	}

}