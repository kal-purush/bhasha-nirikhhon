package com.unionclass.memberservice.agreement.dto.in;

import com.unionclass.memberservice.agreement.vo.in.UpdateAgreementReqVo;
import jakarta.validation.Valid;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
public class UpdateAgreementReqDto {

    private Long agreementUuid;
    private String agreementName;
    private String agreementContent;
    private Boolean required;

    @Builder
    public UpdateAgreementReqDto(Long agreementUuid, String agreementName, String agreementContent, Boolean required) {
        this.agreementUuid = agreementUuid;
        this.agreementName = agreementName;
        this.agreementContent = agreementContent;
        this.required = required;
    }

    public static UpdateAgreementReqDto of(Long agreementUuid, UpdateAgreementReqVo updateAgreementReqVo) {
        return UpdateAgreementReqDto.builder()
                .agreementUuid(agreementUuid)
                .agreementName(updateAgreementReqVo.getAgreementName())
                .agreementContent(updateAgreementReqVo.getAgreementContent())
                .required(updateAgreementReqVo.getRequired())
                .build();
    }
}