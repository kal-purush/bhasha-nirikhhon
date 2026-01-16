package com.jamesmhare.magicmirror.views;

import static org.junit.Assert.assertEquals;

import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class for {@link ApplicationViewImpl}.
 * 
 * @author jameshare
 */
public class ApplicationViewImplTest {
	private Shell shell;
	private Display display;
	private ApplicationViewImpl testApplicationViewImpl;

	/**
	 * Runs before the tests the open a new Shell.
	 */
	@Before
	public void setUp() throws Exception {
		shell = getShell();
	}

	/**
	 * Test to ensure that {@link IllegalArgumentException} is thrown when the
	 * parent shell is null.
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testErrorWhenApplicationViewShellIsNull() {
		new ApplicationViewImpl(null);
	}

	/**
	 * Test to ensure that the minimum size is set with constructor.
	 */
	@Test
	public void testShellMinimumSizeIsSet() {
		testApplicationViewImpl = new ApplicationViewImpl(shell);
		assertEquals(shell.getSize(), shell.getMinimumSize());
	}

	private Shell getShell() {
		if (shell != null && shell.isDisposed()) {
			shell = null;
		}
		Display currentDisplay = (this.display != null) ? this.display : Display.getCurrent();
		if (currentDisplay != null && !currentDisplay.isDisposed()) {
			Shell active = currentDisplay.getActiveShell();
			if (active != null) {
				shell = new Shell(active);
			}
		}
		if (shell == null) {
			shell = new Shell(currentDisplay);
		}
		return shell;
	}

}