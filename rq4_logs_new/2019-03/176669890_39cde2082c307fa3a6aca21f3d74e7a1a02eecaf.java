/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package compilerphase2ver3;

/**
 *
 * @author RJ
 */
import TokenLibrary.TokenTypes;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Hashtable;

public class InhaleText{

    public static Hashtable<String, TokenTypes> ExhaleReserves(){
        Hashtable<String, TokenTypes> reservedWords = new  Hashtable<String, TokenTypes>();
                reservedWords.put("+", TokenTypes.ADDSUBOP);
		reservedWords.put("-", TokenTypes.ADDSUBOP);
		reservedWords.put("/", TokenTypes.MULDIVOP);
                reservedWords.put("//", TokenTypes.COMMENT);
		reservedWords.put("*", TokenTypes.MULDIVOP);
                
                reservedWords.put(">>", TokenTypes.RELOP);
                reservedWords.put("<<", TokenTypes.RELOP);
                reservedWords.put(">>=", TokenTypes.RELOP);
                reservedWords.put("<<=", TokenTypes.RELOP);
                reservedWords.put("==", TokenTypes.RELOP);
                reservedWords.put("!=", TokenTypes.RELOP);
                
                reservedWords.put("::", TokenTypes.ASSIGN_OP);
                
                reservedWords.put("/nl", TokenTypes.NEW_LINE);
                reservedWords.put("/tb", TokenTypes.TAB);
                reservedWords.put("HOLE", TokenTypes.HOLE);
                reservedWords.put("FOLLOW", TokenTypes.FOLL);
                reservedWords.put("name", TokenTypes.LENGTH);
                reservedWords.put("DESC", TokenTypes.DESC_DATA);
                reservedWords.put("INITIAL", TokenTypes.INIT_DATA);
                reservedWords.put("STAT", TokenTypes.STAT_DATA);
                reservedWords.put("FRAC", TokenTypes.FRAC_DATA);
                reservedWords.put("NAME", TokenTypes.NAME_DATA);
                reservedWords.put("DESCv", TokenTypes.DESC_VAR);
                reservedWords.put("INITIALv", TokenTypes.INIT_VAR);
                reservedWords.put("STATv", TokenTypes.STAT_VAR);
                reservedWords.put("FRACv", TokenTypes.FRAC_VAR);
                reservedWords.put("NAMEv", TokenTypes.NAME_VAR);
                reservedWords.put("3.14", TokenTypes.FRAC_LIT);
                reservedWords.put("3", TokenTypes.STAT_LIT);
                //reservedWords.put("a", new NAME_DATA())
                reservedWords.put("string", TokenTypes.NAME_LIT);
                reservedWords.put("TRUE", TokenTypes.DESC_LITt);
                reservedWords.put("FALSE", TokenTypes.DESC_LITf);
                reservedWords.put("LOCAL", TokenTypes.LOC);
                
                reservedWords.put("PATH", TokenTypes.PATHMAIN);
                reservedWords.put("ALTPATH", TokenTypes.PATHMAIN);
                reservedWords.put("ALT", TokenTypes.PATHMAIN);
                reservedWords.put("COMBO", TokenTypes.COMBO_F);
                reservedWords.put("Comboname", TokenTypes.CHAIN);
                //reservedWords.put("<STAT RELOP STAT>", TokenTypes.Token);
                
                reservedWords.put(";", TokenTypes.ENDLINE);
                
                reservedWords.put("PLAYER", TokenTypes.PLAYER_O);
                reservedWords.put("#", TokenTypes.GAME_START);
                reservedWords.put("GAME", TokenTypes.GAME);
                reservedWords.put("ENDGAME", TokenTypes.END_GAME);
                reservedWords.put("$", TokenTypes.ID);
                
                
                reservedWords.put("(", TokenTypes.L_PAR);
                reservedWords.put(")", TokenTypes.R_PAR);
                
                reservedWords.put("DOT_HEAL", TokenTypes.DOTS);
                reservedWords.put("DOT_DAMAGE", TokenTypes.DOTS);
                reservedWords.put("SCALE", TokenTypes.SCL);
                reservedWords.put("SHRINK", TokenTypes.G_EFF);
                reservedWords.put("REMAIN", TokenTypes.G_EFF);
                reservedWords.put("ENLARGE", TokenTypes.G_EFF);
                reservedWords.put("HEAL", TokenTypes.L_EFF);
                reservedWords.put("DAMAGE", TokenTypes.L_EFF);
                reservedWords.put("DN_GR", TokenTypes.GRADE);
                reservedWords.put("UP_GR", TokenTypes.GRADE);
                reservedWords.put("NOT", TokenTypes.NOT);
                
                reservedWords.put("AND", TokenTypes.G_LOG);
                reservedWords.put("NAND", TokenTypes.G_LOG);
                reservedWords.put("OR", TokenTypes.L_LOG);
                reservedWords.put("NOR", TokenTypes.L_LOG);
                reservedWords.put("XOR", TokenTypes.L_LOG);
                reservedWords.put("STAT[", TokenTypes.S_ARRAY);
                reservedWords.put("STAT[][", TokenTypes.M_ARRAY);
                
                reservedWords.put("{", TokenTypes.LEFTBRACE);
                reservedWords.put("}", TokenTypes.RIGHTBRACE);
               // reservedWords.put("CHAIN", new Token());
                //reservedWords.put("", new Token());

        reservedWords.put("Is", TokenTypes.ASSIGN_OP);

        return reservedWords;
    }

    public static PushbackInputStream ExhaleText(String pathDirectory){
        FileReader inputStream = null;
        FileWriter outputStream = null;

        PushbackInputStream fr = null;
        byte[] syntax;

        Path path = Paths.get(pathDirectory);
        try {
            syntax = Files.readAllBytes(path);
            ByteArrayInputStream array = new ByteArrayInputStream(syntax);
            PushbackInputStream push = new PushbackInputStream(array);

            return push;
        } catch (IOException e) {
            e.printStackTrace();
        }


        return null;
    }

}