package com.yearup;

import javax.swing.*;
import java.awt.*;
import java.util.ArrayList;
import java.util.Random;
import java.util.Scanner;

public class VolunteerPickerUI extends JFrame {



    static ArrayList<Student> students;
    static ArrayList<Student> workingList;
    private static final int DELAY_IN_SECONDS = 5;

    private static void showRemainingStudents() {
        System.out.println("There are "+workingList.size()+" remaining Students. They are:");
        for (Student student :
                workingList) {
            System.out.println(student.getFirstName());
        }
    }


    public static void main(String[] args){
        VolunteerPickerUI ui = new VolunteerPickerUI();
        ui.setSize(800,800);
        ui.add(new Panel());
        ui.setDefaultCloseOperation(EXIT_ON_CLOSE);
        SwingUtilities.invokeLater(new Runnable() {
            @Override
            public void run() {
                ui.setVisible(true);
            }
        });
    }

    //-----> drawing panel
    static class Panel extends JPanel{

        private int pos;

        enum UIState{
                INITIAL,
                COUNT_DOWN,
                NAME_DISPLAY,
                NAME_WITH_YESNO
        }
        String displayedName = "";
        UIState uiState = UIState.INITIAL;
        int currentState = 0;
        Button chooseStudentBtn = new Button("Choose Student");
        Button showRemaining = new Button("Show remaining");
        Button yesBtn = new Button("yes");
        Button noBtn = new Button("no");
        String counter = "";
        public Panel(){
            setupStudents();
            transitionToState(UIState.INITIAL);

            chooseStudentBtn.addActionListener(e -> {
                chooseVolunteer();
            });

            yesBtn.addActionListener(e -> {
                workingList.remove(pos);
                transitionToState(UIState.INITIAL);
            });

            noBtn.addActionListener( e -> {
                transitionToState(UIState.INITIAL);
            });

            showRemaining.addActionListener(e->{
                showRemainingStudents();
            });
        }

        private static void setupStudents() {
            students = new ArrayList<>();
            Scanner sc = new Scanner(VolunteerPickerUI.class.getClassLoader().getResourceAsStream("names.txt"));
            while(sc.hasNextLine()){
                String name = sc.nextLine();
                String[] fullName = name.split(" ");
                String firstName = fullName[0];
                String lastName = fullName[fullName.length-1];
                students.add(new Student(firstName, lastName));
            }
            workingList = (ArrayList<Student>) students.clone();

        }

        private void chooseVolunteer() {
            if(workingList == null || workingList.isEmpty())
                workingList = (ArrayList<Student>)students.clone();

            Random r = new Random();
            pos = r.nextInt(workingList.size());
            transitionToState(UIState.COUNT_DOWN);
            counter = "...";
            new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i<= DELAY_IN_SECONDS; i++){
                        try {
                            Thread.sleep(1000);
                            counter = String.valueOf(DELAY_IN_SECONDS- i);
                            repaint();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    transitionToState(UIState.NAME_DISPLAY);
                    try {
                        Thread.sleep(5000);
                        transitionToState(UIState.NAME_WITH_YESNO);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }).start();
        }


        public void transitionToState(UIState newState){
            UIState prevState = uiState;
            uiState = newState;
            //set buttons
            switch (uiState){
                case INITIAL:{
                    add(chooseStudentBtn);
                    add(showRemaining);
                    remove(yesBtn);
                    remove(noBtn);
                    break;
                }
                case COUNT_DOWN:{
                    remove(chooseStudentBtn);
                    remove(showRemaining);
                    break;
                }
                case NAME_DISPLAY:{
                    counter = "";
                    break;
                }
                case NAME_WITH_YESNO:{
                    add(yesBtn);
                    add(noBtn);
                }
            }
            repaint();
        }


        @Override
        public void paint(Graphics g) {
            super.paint(g);
            g.setColor(Color.decode("#698057"));
            Graphics2D g2d = (Graphics2D) g;
            int width = getWidth();
            int height = getHeight();
            int centerX = width/2;
            int centerY = height/2;
            g.fillRect(0,0,width,height);
            g.setColor(Color.BLACK);
            switch (uiState){
                case INITIAL: {
                    g.setFont(g.getFont().deriveFont(height/20f));
                    FontMetrics metrics = g2d.getFontMetrics();
                    int cbs= metrics.stringWidth(" Choose Student ");
                    chooseStudentBtn.setBounds(centerX - cbs/2, centerY,cbs,height/20);
                    showRemaining.setBounds(centerX - cbs/2, height-height/20,cbs,height/20);
                    break;
                }
                case COUNT_DOWN: {
                    g.setFont(g.getFont().deriveFont(height/2f));
                    FontMetrics metrics = g2d.getFontMetrics();
                    int measuredWidth = metrics.stringWidth(counter);
                    g2d.drawString(counter,centerX - measuredWidth/2, centerY + height/8f);
                    break;
                }
                case NAME_WITH_YESNO:{
                    g.setFont(g.getFont().deriveFont(height/20f));
                    String message = "Did they answer correctly? :";
                    g.setColor(Color.decode("#f0b241"));
                    g2d.drawString(message,0,centerY + height/10f);
                    yesBtn.setBounds(centerX - width/10,centerY + height/10 + height/20,width/10,height/20);
                    noBtn.setBounds(centerX ,centerY + height/10 + height/20,width/10,height/20);

                }
                case NAME_DISPLAY:{
                    g.setFont(g.getFont().deriveFont(height/10f));
                    FontMetrics metrics = g2d.getFontMetrics();
                    String name = workingList.get(pos).getFirstName();
                    int measuredWidth = metrics.stringWidth(name);
                    g.setColor(Color.decode("#331b00"));
                    g2d.drawString(name,centerX -measuredWidth/2, centerY - height/10f);
                    break;
                }
            }
        }

        //--------->

    }

}