package it.gov.pagopa.pu.debtpositions.mapper;

import it.gov.pagopa.pu.debtpositions.dto.Installment;
import it.gov.pagopa.pu.debtpositions.dto.generated.DebtPositionDTO;
import it.gov.pagopa.pu.debtpositions.model.DebtPosition;
import it.gov.pagopa.pu.debtpositions.model.InstallmentNoPII;
import it.gov.pagopa.pu.debtpositions.model.PaymentOption;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

@Service
public class DebtPositionMapper {

  private final PaymentOptionMapper paymentOptionMapper;

  private static final Collector<PaymentOption, ?, SortedSet<PaymentOption>> toPaymentOptionTreeSet = Collectors.toCollection(TreeSet::new);

  public DebtPositionMapper(PaymentOptionMapper paymentOptionMapper) {
    this.paymentOptionMapper = paymentOptionMapper;
  }

  public Pair<DebtPosition, Map<InstallmentNoPII, Installment>> mapToModel(DebtPositionDTO dto) {
    DebtPosition debtPosition = new DebtPosition();
    debtPosition.setDebtPositionId(dto.getDebtPositionId());
    debtPosition.setIupdOrg(dto.getIupdOrg());
    debtPosition.setDescription(dto.getDescription());
    debtPosition.setStatus(dto.getStatus());
    debtPosition.setIngestionFlowFileId(dto.getIngestionFlowFileId());
    debtPosition.setIngestionFlowFileLineNumber(dto.getIngestionFlowFileLineNumber());
    debtPosition.setOrganizationId(dto.getOrganizationId());
    debtPosition.setDebtPositionTypeOrgId(dto.getDebtPositionTypeOrgId());
    debtPosition.setNotificationDate(dto.getNotificationDate());
    debtPosition.setValidityDate(dto.getValidityDate());
    debtPosition.setFlagIuvVolatile(dto.getFlagIuvVolatile());
    debtPosition.setCreationDate(dto.getCreationDate().toLocalDateTime());
    debtPosition.setUpdateDate(dto.getUpdateDate().toLocalDateTime());

    Map<InstallmentNoPII, Installment> installmentMapping = new HashMap<>();

    SortedSet<PaymentOption> paymentOptions = dto.getPaymentOptions().stream()
      .map(paymentOptionDTO -> {
        Pair<PaymentOption, Map<InstallmentNoPII, Installment>> paymentOptionWithInstallments = paymentOptionMapper.mapToModel(paymentOptionDTO);
        installmentMapping.putAll(paymentOptionWithInstallments.getSecond());
        return paymentOptionWithInstallments.getFirst();
      })
      .collect(toPaymentOptionTreeSet);

    debtPosition.setPaymentOptions(paymentOptions);

    return Pair.of(debtPosition, installmentMapping);
  }
}