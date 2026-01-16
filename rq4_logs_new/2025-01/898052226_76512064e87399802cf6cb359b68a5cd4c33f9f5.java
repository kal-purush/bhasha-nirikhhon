package it.gov.pagopa.pu.debtpositions.enums;

import it.gov.pagopa.pu.debtpositions.dto.generated.PersonDTO;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class PersonEntityTypeEnumTest {

  @Test
  void testConversion(){
    for (PersonEntityType value : PersonEntityType.values()) {
      Assertions.assertDoesNotThrow(() -> PersonDTO.EntityTypeEnum.valueOf(value.name()));
    }
  }
}