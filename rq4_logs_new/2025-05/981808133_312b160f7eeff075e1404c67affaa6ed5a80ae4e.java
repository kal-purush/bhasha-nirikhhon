package com.unionclass.memberservice.agreement.dto.out;

import com.unionclass.memberservice.agreement.entity.Agreement;
import com.unionclass.memberservice.agreement.vo.out.GetAgreementResVo;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
public class GetAgreementResDto {

    private String agreementName;
    private String agreementContent;
    private Boolean required;

    @Builder
    public GetAgreementResDto(String agreementName, String agreementContent, Boolean required) {
        this.agreementName = agreementName;
        this.agreementContent = agreementContent;
        this.required = required;
    }

    public static GetAgreementResDto from(Agreement agreement) {
        return GetAgreementResDto.builder()
                .agreementName(agreement.getName())
                .agreementContent(agreement.getContent())
                .required(agreement.getRequired())
                .build();
    }

    public GetAgreementResVo toVo() {
        return GetAgreementResVo.builder()
                .agreementName(agreementName)
                .agreementContent(agreementContent)
                .required(required)
                .build();
    }
}