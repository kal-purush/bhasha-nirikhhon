/*
    Author:         Samuel Larsson
    Copyright:      2018
    Description:    A program that replicates a file but replaces all characters
                    that aren't alphabetical, space or new line with blanks.

    Dependancies:   StdIn.java
    Compilation:    javac -d . OnlyAlphaBlank.java StdIn.java
    Execution:      java com.company.OnlyAlphaBlank < inputFilename > outputFilename
    Usage:          Replace inputFilename with the file that should be replicated
                    and replace outputFilename with where the result should go.
 */

package com.company;

class OnlyAlphaBlank {
    private static boolean isAlpha(char c) {
        return Character.isLetter(c);
    }

    public static void main(String[] args) {
        char c;
        while(!StdIn.isEmpty()) {
            c = StdIn.readChar();
            if(isAlpha(c) || c == ' ' || c == '\n')
                System.out.print(c);
            else
                System.out.print(' ');
        }
    }
}