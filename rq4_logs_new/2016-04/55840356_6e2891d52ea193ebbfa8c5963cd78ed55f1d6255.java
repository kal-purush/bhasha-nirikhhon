package Notepad;

import util.BraceChecker;

import javax.swing.*;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.*;
import java.util.Properties;


enum LabelKey {


}

public class Notepad extends JFrame {


    public Notepad() {

    }




    class NotepadMenuBar extends JMenuBar {

        public NotepadMenuBar (){



        }

        private void init(LanguageType languageType){

        }

        private void loadMenusLabels(LanguageType languageType){

        }

        private void loadMenusLabels(Properties label){

        }

        private void loadMessage(LanguageType languageType){

        }

        private void loadMessage(Properties label){
        }


        private Properties getLabelProperties(LanguageType languageType){

            return null;
        }

        private void newActionPerformed (ActionEvent e) {

        }

        private void realiseNewOptions(){

        }

        private void openActionPerformed(ActionEvent e) {


        }

        private void openWindowActions(){

        }

        private void saveActionPerformed(ActionEvent e) {

        }

        private void saveAsActionPerformed(ActionEvent e) {

        }

        private void exitActionPerformed(ActionEvent e) {


        }

        private boolean checkForSave() {
            return false;
        }

        private void changeTitle() {
        }

        private String reader(File currentFile) {

           return null;
        }



    }
    enum LanguageType {


    }




    public static void main(String[] args) {
        new Notepad();
    }

}
