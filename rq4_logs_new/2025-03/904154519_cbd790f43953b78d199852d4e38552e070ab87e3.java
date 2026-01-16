package it.gov.pagopa.pu.classification.dto;

import it.gov.pagopa.pu.classification.model.NoPIIEntity;

public interface FullPIIDTO<E extends NoPIIEntity<P>, P extends PIIDTO> {
  E getNoPII();
  void setNoPII(E noPII);
}