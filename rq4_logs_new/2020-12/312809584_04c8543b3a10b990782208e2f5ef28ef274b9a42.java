/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.andy.javafxtest;
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import java.io.IOException;
import java.io.OutputStream;
import javafx.scene.control.TextArea;

/**
 *
 * @author maria
 */
public class customOutputStream extends OutputStream {
    private TextArea textArea;
     
    public customOutputStream(TextArea textArea) {
        this.textArea = textArea;
    }
     
    @Override
    public void write(int b) throws IOException {
        // redirects data to the text area
        textArea.appendText(String.valueOf((char)b));
        // scrolls the text area to the end of data
        textArea.positionCaret(textArea.getCaretPosition());
    }
}