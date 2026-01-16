package com.seachangesimulations.scorp.domain;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.MappedSuperclass;

/** This class is the base class for all scorp domain (pojo) classes such as Actor or Phase.
 * @author Mike Sheliga - 6.21.18
 */
@MappedSuperclass  // No separate table for this class.
public abstract class BaseSCObject {

	@Id
	@GeneratedValue
	private Long id;  // Must be in base class for setting inside generic create and update.

	public Long getId() {return id;}
	public void setId(Long id) {this.id = id;}

} // end class BaseSCObject