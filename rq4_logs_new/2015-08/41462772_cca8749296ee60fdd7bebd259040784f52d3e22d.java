/**
 * Copyright 2011 GNF
 * @author ivmedina
 * 
 */
package gnf.gido.common.batch.log.convert;

import gnf.gido.common.batch.log.convert.job.JobConvertInterface;
import gnf.gido.common.batch.log.convert.step.StepConvertInterface;
import gnf.gido.common.batch.log.data.ConvertData;
import gnf.gido.common.batch.log.data.job.JobConvertDataInterface;
import gnf.gido.common.batch.log.data.step.StepConvertDataInterface;

import java.util.ArrayList;
import java.util.Collection;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.util.Assert;

/**
 * The Class Convert.
 */
public final class Convert {

	/** The Constant LOG_PARAM. */
	public final static String LOG_PARAM = ".log";

	/** The Constant LOG_PARAM_ARRAY. */
	public final static String LOG_PARAM_ARRAY = "log.array";

	/** The Constant LOG_PARAM_VISIBLE. */
	public final static String LOG_PARAM_VISIBLE = "log.visible";

	/** The Constant LOG_PARAM_LEIDOS. */
	public final static String LOG_PARAM_LEIDOS = "log.leidos";

	/** The Constant LOG_PARAM_RECHAZADOS. */
	public final static String LOG_PARAM_RECHAZADOS = "log.rechazados";

	/** The Constant LOG_PARAM_PROCESADOS. */
	public final static String LOG_PARAM_PROCESADOS = "log.procesados";

	/** The Constant LOG_JOB_DESCRIPTION. */
	public final static String LOG_JOB_DESCRIPTION = "log.job.description";

	/** The Constant LOG_JOB_ERROR. */
	public final static String LOG_JOB_ERROR = "log.job.error";

	private JobConvertInterface jobConvert;

	private StepConvertInterface stepConvert;

	/**
	 * Convert.
	 * 
	 * @param jobExecution the job execution
	 * @return the convert data
	 */
	public ConvertData convert(final JobExecution jobExecution) {
		Assert.notNull(jobExecution);

		return new ConvertData(convertJobContext(jobExecution), convertStepsContext(jobExecution.getStepExecutions()));
	}

	private JobConvertDataInterface convertJobContext(final JobExecution jobExecution) {
		return getJobConvert().convert(jobExecution);
	}

	private Collection<StepConvertDataInterface> convertStepsContext(final Collection<StepExecution> stepsContext) {
		final Collection<StepConvertDataInterface> list = new ArrayList<StepConvertDataInterface>();
		for (final StepExecution context : stepsContext) {
			list.add(convertStepContext(context));
		}
		return list;
	}

	private StepConvertDataInterface convertStepContext(final StepExecution context) {
		return getStepConvert().convert(context);
	}

	/**
	 * Gets the job convert.
	 * 
	 * @return the job convert
	 */
	public JobConvertInterface getJobConvert() {
		return jobConvert;
	}

	/**
	 * Sets the job convert.
	 * 
	 * @param jobContextLogger the new job convert
	 */
	public void setJobConvert(final JobConvertInterface jobContextLogger) {
		jobConvert = jobContextLogger;
	}

	/**
	 * Gets the step convert.
	 * 
	 * @return the step convert
	 */
	public StepConvertInterface getStepConvert() {
		return stepConvert;
	}

	/**
	 * Sets the step convert.
	 * 
	 * @param stepContextLogger the new step convert
	 */
	public void setStepConvert(final StepConvertInterface stepContextLogger) {
		stepConvert = stepContextLogger;
	}
}