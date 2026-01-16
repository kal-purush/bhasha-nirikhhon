package prd.fms.executor;

import java.awt.event.WindowEvent;

import prd.fms.view.BaseWindowListener;

public class MainFrameWindowExecutor extends BaseWindowListener{

	private Thread t;
	
	public MainFrameWindowExecutor(Thread t) {
		this.t = t;
	}
	
	@Override
	protected void executeClosed(WindowEvent e) {
		t.interrupt();
		System.exit(0);
	}

}