package data.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Inheritance;
import jakarta.persistence.InheritanceType;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
@IdClass(ExecutionRecordIdentifier.class)
@Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
public class ExecutionRecordIdentifier {
  @Id
  @Column(length = 20)
  private String datasetId;
  @Id
  @Column(length = 50)
  private String executionId;
  @Id
  @Column(length = 300)
  private String recordId;

}