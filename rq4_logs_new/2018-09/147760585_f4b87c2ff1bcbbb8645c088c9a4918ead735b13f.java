package com.jamesmhare.magicmirror.application;

import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;

import com.jamesmhare.magicmirror.views.ApplicationView;
import com.jamesmhare.magicmirror.views.ApplicationViewImpl;

public class Application {

	public static void main(String[] args) {
		Display display = new Display();
		Shell shell = new Shell(display, SWT.BORDER | SWT.CLOSE | SWT.MIN | SWT.MAX | SWT.RESIZE);
		ApplicationView applicationView = new ApplicationViewImpl(shell);
		shell.pack();
		shell.open();
		while (!shell.isDisposed()) {
			if (!display.readAndDispatch()) {
				display.sleep();
			}
		}
		display.dispose();

	}

}