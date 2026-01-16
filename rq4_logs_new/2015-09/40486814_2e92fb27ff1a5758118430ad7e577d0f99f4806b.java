package org.hydra.tasker.db.beans;

import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "task")
public class Task {
	@Id
	private ObjectId id;
	private String userId;
	private String title;
	private String desc;
	private boolean fire;
	private boolean important;
	private String progress;
	private boolean play;
	private List<Task> subTasks;

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
	 * @return the progress
	 */
	public String getProgress() {
		return progress;
	}

	/**
	 * @param progress
	 *            the progress to set
	 */
	public void setProgress(String progress) {
		this.progress = progress;
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

	/**
	 * @return the subTasks
	 */
	public List<Task> getSubTasks() {
		return subTasks;
	}

	/**
	 * @param subTasks
	 *            the subTasks to set
	 */
	public void setSubTasks(List<Task> subTasks) {
		this.subTasks = subTasks;
	}

	/**
	 * @return the userId
	 */
	public String getUserId() {
		return userId;
	}

	/**
	 * @param userId
	 *            the userId to set
	 */
	public void setUserId(String userId) {
		this.userId = userId;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Task [id=" + id + ", userId=" + userId + ", title=" + title
				+ ", desc=" + desc + ", fire=" + fire + ", important="
				+ important + ", progress=" + progress + ", play=" + play
				+ ", subTasks=" + subTasks + "]";
	}

	/**
	 * @return the id
	 */
	public ObjectId getId() {
		return id;
	}

	/**
	 * @param id
	 *            the id to set
	 */
	public void setId(ObjectId id) {
		this.id = id;
	}
}