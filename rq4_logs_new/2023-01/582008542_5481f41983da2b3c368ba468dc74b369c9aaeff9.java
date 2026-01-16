package by.teachmeskills.strings;

public class StringDemo {
    public static void main(String[] args) {
        final String document = "8887-ABC-6666-def-7d8j";
        StringValidation.printFirstTwoBlocksWithFourDigits(document);
        StringValidation.replaceWordBlocksWithAsterisk(document);
        StringValidation.printOnlyWordsInCertainFormat(document);
        StringValidation.printOnlyWordsInCertainFormatInUpperCaseWithStringBuilder(document);
        StringValidation.findAbcSequenceInDocument(document);
        StringValidation.findIfDocumentStartsWith555(document);
        StringValidation.findIfDocumentEndsWith1a2b(document);
    }
}