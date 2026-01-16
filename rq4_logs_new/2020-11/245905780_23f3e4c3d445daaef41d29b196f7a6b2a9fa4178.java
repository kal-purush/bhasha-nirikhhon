package hyve.petshow.mock;

import hyve.petshow.controller.representation.MensagemRepresentation;

public class MensagemMock {
    public static MensagemRepresentation mensagemRepresentationSucesso(){
        var mensagemRepresentation = new MensagemRepresentation(1L);

        mensagemRepresentation.setSucesso(Boolean.TRUE);

        return mensagemRepresentation;
    }

    public static MensagemRepresentation mensagemRepresentationFalha(){
        var mensagemRepresentation = new MensagemRepresentation(1L);

        mensagemRepresentation.setSucesso(Boolean.FALSE);

        return mensagemRepresentation;
    }
}