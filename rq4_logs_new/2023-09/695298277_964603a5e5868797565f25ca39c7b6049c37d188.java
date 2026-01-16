package convertisseur;

public class Branch extends FonctionsConvert{
    public Branch(String[] argument) {
        debutChaine = "11";
        listArgument = argument;
    }

    private String cond() {
        String condition = listArgument[0].substring(1);
        return switch (condition) {
            case "EQ", "eq" -> "0000";
            case "NE", "ne" -> "0001";
            case "CS", "cs", "HS", "hs" -> "0010";
            case "CC", "cc", "LO", "lo" -> "0011";
            case "MI", "mi" -> "0100";
            case "PL", "pl" -> "0101";
            case "VS", "vs" -> "0110";
            case "VC", "vc" -> "0111";
            case "HI", "hi" -> "1000";
            case "LS", "ls" -> "1001";
            case "GE", "ge" -> "1010";
            case "LT", "lt" -> "1011";
            case "GT", "gt" -> "1100";
            case "LE", "le" -> "1101";
            case "AL", "al" -> "1110";
            default -> "1111";
        };
    }

    public String convertBranch(int imm) {
        int entier;
        if (listArgument[1].charAt(1) == '-') entier = -1 * ConvertToBits.convertStrToInt( listArgument[1].substring(2) );
        else entier = ConvertToBits.convertStrToInt( listArgument[1].substring(1) );
        return ConvertToBits.convertIntToBinaireComplementary((short) entier, (short) imm);
    }

    @Override
    String traiterNomAssembleur() {
        if (listArgument[0].length() == 1) return " | 100 | " + convertBranch(11);
        else return " | 01 | " + cond() + " | " + convertBranch(8);
    }
}