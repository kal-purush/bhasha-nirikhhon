package uk.gov.hmcts.reform.pip.channel.management.models;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Getter
public class PublicationFiles {
    private byte[] primaryPdf;
    private byte[] additionalPdf;
    private byte[] excel;
}