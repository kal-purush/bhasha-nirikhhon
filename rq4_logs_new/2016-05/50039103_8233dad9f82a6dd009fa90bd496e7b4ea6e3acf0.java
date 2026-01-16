package com.zhxia.quartz.listener;

import java.util.Date;

import org.quartz.JobExecutionContext;
import org.quartz.Trigger;
import org.quartz.Trigger.CompletedExecutionInstruction;
import org.quartz.listeners.TriggerListenerSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobTriggerListener extends TriggerListenerSupport {
	private final static Logger logger = LoggerFactory.getLogger(JobTriggerListener.class);

	@Override
	public String getName() {
		return this.getClass().getName();
	}

	@Override
	public void triggerFired(Trigger trigger, JobExecutionContext context) {
		long fireTime = System.currentTimeMillis();
		logger.info(String.format("job fire at:%s", new Date(fireTime)));
	}

	@Override
	public boolean vetoJobExecution(Trigger trigger, JobExecutionContext context) {
		// TODO Auto-generated method stub
		return super.vetoJobExecution(trigger, context);
	}

	@Override
	public void triggerMisfired(Trigger trigger) {
		// TODO Auto-generated method stub
		super.triggerMisfired(trigger);
	}

	@Override
	public void triggerComplete(Trigger trigger, JobExecutionContext context,
			CompletedExecutionInstruction triggerInstructionCode) {
		// TODO Auto-generated method stub
		super.triggerComplete(trigger, context, triggerInstructionCode);
	}

}