/*
 * Copyright (c) 2015-2016 Meplato GmbH, Switzerland.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.meplato.store2.catalogs;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.annotations.SerializedName;

/**
 * Project describes customer-specific settings, typically encompassing a set of
 * catalogs.
 */
public class Project {
	@SerializedName("country")
	private String country;
	@SerializedName("created")
	private Date created;
	@SerializedName("id")
	private long id;
	@SerializedName("kind")
	private String kind;
	@SerializedName("language")
	private String language;
	@SerializedName("mpbc")
	private String mpbc;
	@SerializedName("mpcc")
	private String mpcc;
	@SerializedName("name")
	private String name;
	@SerializedName("selfLink")
	private String selfLink;
	@SerializedName("type")
	private String type;
	@SerializedName("updated")
	private Date updated;
	@SerializedName("visible")
	private boolean visible;

	/**
	 * Create new instance of Project.
	 */
	public Project() {
	}

	/**
	 * Country specifies the country code where catalogs for this project are
	 * located.
	 */
	public String getCountry() {
		return this.country;
	}

	/**
	 * Country specifies the country code where catalogs for this project are
	 * located.
	 */
	public void setCountry(String country) {
		this.country = country;
	}

	/**
	 * Created is the creation date and time of the project.
	 */
	public Date getCreated() {
		return this.created;
	}

	/**
	 * Created is the creation date and time of the project.
	 */
	public void setCreated(Date created) {
		this.created = created;
	}

	/**
	 * ID is a unique (internal) identifier of the project.
	 */
	public long getId() {
		return this.id;
	}

	/**
	 * ID is a unique (internal) identifier of the project.
	 */
	public void setId(long id) {
		this.id = id;
	}

	/**
	 * Kind is store#project for a project entity.
	 */
	public String getKind() {
		return this.kind;
	}

	/**
	 * Kind is store#project for a project entity.
	 */
	public void setKind(String kind) {
		this.kind = kind;
	}

	/**
	 * Language specifies the language code of the catalogs of this project.
	 */
	public String getLanguage() {
		return this.language;
	}

	/**
	 * Language specifies the language code of the catalogs of this project.
	 */
	public void setLanguage(String language) {
		this.language = language;
	}

	/**
	 * MPBC is the Meplato Buyer Code that identifies a set of buy-side companies
	 * that belong together.
	 */
	public String getMpbc() {
		return this.mpbc;
	}

	/**
	 * MPBC is the Meplato Buyer Code that identifies a set of buy-side companies
	 * that belong together.
	 */
	public void setMpbc(String mpbc) {
		this.mpbc = mpbc;
	}

	/**
	 * MPCC is the Meplato Company Code that uniquely identifies the buy-side.
	 */
	public String getMpcc() {
		return this.mpcc;
	}

	/**
	 * MPCC is the Meplato Company Code that uniquely identifies the buy-side.
	 */
	public void setMpcc(String mpcc) {
		this.mpcc = mpcc;
	}

	/**
	 * Name is a short description of the project.
	 */
	public String getName() {
		return this.name;
	}

	/**
	 * Name is a short description of the project.
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * URL to this page.
	 */
	public String getSelfLink() {
		return this.selfLink;
	}

	/**
	 * URL to this page.
	 */
	public void setSelfLink(String selfLink) {
		this.selfLink = selfLink;
	}

	/**
	 * Type describes the type of project which can be either corporate or basic.
	 */
	public String getType() {
		return this.type;
	}

	/**
	 * Type describes the type of project which can be either corporate or basic.
	 */
	public void setType(String type) {
		this.type = type;
	}

	/**
	 * Updated is the last modification date and time of the project.
	 */
	public Date getUpdated() {
		return this.updated;
	}

	/**
	 * Updated is the last modification date and time of the project.
	 */
	public void setUpdated(Date updated) {
		this.updated = updated;
	}

	/**
	 * Visible indicates whether this project is visible to merchants.
	 */
	public boolean isVisible() {
		return this.visible;
	}

	/**
	 * Visible indicates whether this project is visible to merchants.
	 */
	public void setVisible(boolean visible) {
		this.visible = visible;
	}
}
