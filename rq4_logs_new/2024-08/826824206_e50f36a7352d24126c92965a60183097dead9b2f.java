package br.com.fiap.totem_express.domain.user;

public record CPF(String value) {
    private static final String CPF_REGEX_RAW_PATTERN = "(\\d{3})(\\d{3})(\\d{3})(\\d{2})";
    private static final String CPF_REGEX_FORMAT = "$1.$2.$3-$4";

    public String formatted(){
        return value.replaceAll(CPF_REGEX_RAW_PATTERN, CPF_REGEX_FORMAT);
    }
}