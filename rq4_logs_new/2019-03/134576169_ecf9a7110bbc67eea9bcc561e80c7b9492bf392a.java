package io.github.cloudiator.persistance;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import java.io.Serializable;
import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.MappedSuperclass;

/**
 * Superclass for all model classes. Defines the auto generated id for each model class in
 * Monitoring. same as is common only separated
 */
@MappedSuperclass
abstract class BaseModel implements Serializable {

  @Id
  @GeneratedValue(strategy = GenerationType.SEQUENCE)
  @Column(name = "id", updatable = false, nullable = false)
  private Long id;

  /**
   * Empty constructor for hibernate.
   */
  protected BaseModel() {
  }

  /**
   * Getter for the id.
   *
   * @return the identifies for this model object.
   */
  public Long getId() {
    return id;
  }

  /**
   * Setter for the id.
   *
   * @param id the identified for this model object
   */
  public void setId(Long id) {
    this.id = id;
  }

  protected ToStringHelper stringHelper() {
    return MoreObjects.toStringHelper(this).add("id", id);
  }

  @Override
  public String toString() {
    return stringHelper().toString();
  }
}