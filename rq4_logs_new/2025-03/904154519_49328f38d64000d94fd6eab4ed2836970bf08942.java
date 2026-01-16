package it.gov.pagopa.pu.classification.citizen.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Entity
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PersonalData {
  @Id
  @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "personal_data_generator")
  @SequenceGenerator(name = "personal_data_generator", sequenceName = "personal_data_id_seq", allocationSize = 1)
  private Long id;
  private String type;
  private byte[] data;
}