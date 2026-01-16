package org.hydra.web.rest.beans;

public enum TaskProgress {
	STOP("stop"), PAUSE("pause"), START("start"), COMPLETE("complete");

	private String progress;

	private TaskProgress(String progress) {
		this.progress = progress;
	}

	/**
	 * @return the progress
	 */
	public String getProgress() {
		return progress;
	}

	public String toString() {
		return progress;
	}
}