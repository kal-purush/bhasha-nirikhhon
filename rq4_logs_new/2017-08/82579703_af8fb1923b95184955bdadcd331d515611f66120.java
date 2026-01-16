package com.superschach.superschach.gui.cli;

import org.apache.log4j.Logger;

public class CLI {
	private Logger logger=Logger.getLogger(CLI.class);
	public CLI(String[] args)
	{
		logger.info("cli gestartet");
	}
}