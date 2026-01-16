package org.sandromax.fastest.domain;

import java.util.LinkedList;

/**
 * @author Oleksandr Maksymenko 2017.01.09
 */
public class Issue {
        //    private Locale language;
        private String subject;
        public String theme;
        private String question;
        public String rightAnswer;
        private LinkedList<String> answers;

        public Issue() {
//        language = new Locale("uk");
            subject = "";
            theme = "";
            question = "";
            rightAnswer = "";
            answers = new LinkedList<>();
        }

        public Issue(String subject, String theme, String question, String rightAnswer, LinkedList<String> answers) {
//        this.language = language;
            this.subject = subject;
            this.theme = theme;
            this.question = question;
            this.rightAnswer = rightAnswer;
            this.answers = answers;
        }

        @Override
        public String toString() {
            return "\nsubject: "+subject+"\ntheme: "+theme+"\nquestion: "+question+"\nright: "+rightAnswer+"\nanswers: "+answers.toString()+"\n";
        }
}