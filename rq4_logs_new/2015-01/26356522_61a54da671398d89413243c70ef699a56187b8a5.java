package minechemV6Base.utils;

import minechemV6Base.chemical.ChemicalBase;
import minechemV6Base.chemical.Element;
import minechemV6Base.chemical.Molecule;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringParser
{
    /**
     * Pattern for detecting molecules from strings (equation safe)
     * Group 1: Matches the Number of Molecules (in an equation) eg 2He
     * Group 2: Matches a Elemental Symbols eg. "Na"
     * Group 3: Matches a molecule grouping without the brackets eg (H2O)=>H2O
     * Group 4: Matches a @(Chemical name) name to allow for shorthand so @water=>water=>H2O
     * Group 5: Matches the multiplier for the preceding element or molecule eg. H2=> H * 2 or C3PO=>(C * 3)(P * 1)(O * 1)
     */
    public static Pattern molecule = Pattern.compile("(\\d*)(?:([A-Z][a-z]{0,2})|(?:\\(([^+=\\s]+)\\))|(?:@([^\\d\\s]+)))(\\d*)");

    public static Molecule parseMolecule(String formula)
    {
        if (isNull(formula)) return null;
        Map<ChemicalBase,Integer> molecules = new HashMap<ChemicalBase, Integer>();
        Matcher matcher = molecule.matcher(formula);
        while (matcher.find())
        {
            int multiplier = toInteger(matcher.group(5));
            Element element = toElement(matcher.group(2));
            if (put(molecules,element,multiplier))continue;
            Molecule chemMolecule = parseMolecule(matcher.group(3));
            if (put(molecules,chemMolecule,multiplier))continue;
            Molecule definedMolecule = toMolecule(matcher.group(4));
            if (put(molecules,definedMolecule,multiplier))continue;
            throw new NullPointerException("Error parsing "+formula);
        }
        return new Molecule(null, (Map.Entry<ChemicalBase,Integer>[])molecules.entrySet().toArray());
    }

    public static final Matcher shells = Pattern.compile("(\\d+)([spdfg])").matcher("1s2s2p3s3p4s3d4p5s4d5p6s4f5d6p7s5f6d7p8s5g6f7d8p9s"); //Handles up to atomic number 170
    public static final int[] subshellElectrons = new int[]{2,6,10,14,18};
    private static final String subShellString = "spdfg";

    public static void getValenceShell(int electronCount)
    {
        shells.reset();
        int shell = 0, subShell = 0;
        while (electronCount>0 && shells.find())
        {
            shell = toInteger(shells.group(1));
            subShell = getSubshell(shells.group(2));
            electronCount-=subshellElectrons[subShell];
        }
        int valenceElectron = subshellElectrons[subShell] + electronCount;
        String valenceShellString = shells.group(0)+valenceElectron;
        return;
    }

    public static boolean put(Map<ChemicalBase, Integer> map, ChemicalBase chemical, int multiplier)
    {
        if (chemical==null) return false;
        map.put(chemical,multiplier);
        return true;
    }

    public static int toInteger(String string)
    {
        if (isNull(string)) return 1;
        return Integer.valueOf(string);
    }

    public static Element toElement(String string)
    {
        return null;
    }

    public static Molecule toMolecule(String string)
    {
        return null;
    }

    public static boolean isNull(String string)
    {
        return string == null || string.isEmpty();
    }

    public static int getSubshell(String s)
    {
        return subShellString.indexOf(s);
    }
}