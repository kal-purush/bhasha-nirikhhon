package org.hydra.tasker.db.beans;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "subtask")
public class SubTask extends Task {
	@Id
	private String subTaskId;
	private String title;
	private String desc;
	private boolean fire;
	private boolean important;
	private boolean play;

	/**
	 * @return the subTaskId
	 */
	public String getSubTaskId() {
		return subTaskId;
	}

	/**
	 * @param subTaskId
	 *            the subTaskId to set
	 */
	public void setSubTaskId(String subTaskId) {
		this.subTaskId = subTaskId;
	}

	/**
	 * @return the title
	 */
	public String getTitle() {
		return title;
	}

	/**
	 * @param title
	 *            the title to set
	 */
	public void setTitle(String title) {
		this.title = title;
	}

	/**
	 * @return the desc
	 */
	public String getDesc() {
		return desc;
	}

	/**
	 * @param desc
	 *            the desc to set
	 */
	public void setDesc(String desc) {
		this.desc = desc;
	}

	/**
	 * @return the fire
	 */
	public boolean isFire() {
		return fire;
	}

	/**
	 * @param fire
	 *            the fire to set
	 */
	public void setFire(boolean fire) {
		this.fire = fire;
	}

	/**
	 * @return the important
	 */
	public boolean isImportant() {
		return important;
	}

	/**
	 * @param important
	 *            the important to set
	 */
	public void setImportant(boolean important) {
		this.important = important;
	}

	/**
	 * @return the play
	 */
	public boolean isPlay() {
		return play;
	}

	/**
	 * @param play
	 *            the play to set
	 */
	public void setPlay(boolean play) {
		this.play = play;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "SubTask [subTaskId=" + subTaskId + ", title=" + title
				+ ", desc=" + desc + ", fire=" + fire + ", important="
				+ important + ", play=" + play + "]";
	}
}