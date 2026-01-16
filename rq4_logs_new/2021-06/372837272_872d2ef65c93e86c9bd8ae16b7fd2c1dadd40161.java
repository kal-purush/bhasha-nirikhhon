package com.example.demo;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class MyFrame extends JFrame implements ActionListener {

    SimpleDateFormat timeFormat;
    SimpleDateFormat dayFormat;
    SimpleDateFormat dateFormat;
    SimpleDateFormat finishTime;

    JLabel finishTimeLabel;
    JButton timeButton;
    JButton sleepTimerButton;
    JLabel dayLabel;
    JLabel dateLabel;

    String time;
    String day;
    String date;
    String kiki=null;

    MyFrame(){

        this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        this.setTitle("Good Night");
        this.setLayout(new FlowLayout());
        this.setSize(350,550);
        this.setResizable(true);

        timeFormat = new SimpleDateFormat("hh:mm:ss");
        finishTime = new SimpleDateFormat("hh:mm:ss");

        dayFormat = new SimpleDateFormat("EEEE");
        dateFormat = new SimpleDateFormat("MMMMM/dd/yy");


        finishTimeLabel = new JLabel();
        finishTimeLabel.setFont(new Font("Verdana", Font.PLAIN,50));
        finishTimeLabel.setForeground(new Color(0x00FF00));
        finishTimeLabel.setBackground(Color.black);
        finishTimeLabel.setOpaque(true);
        finishTimeLabel.setVisible(false);

        timeButton = new JButton();
        timeButton.setSize(200,200);
        timeButton.setFont(new Font("Verdana", Font.PLAIN,50));
        timeButton.setForeground(new Color(0x00FF00));
        timeButton.setBackground(Color.black);
        timeButton.setOpaque(true);

        sleepTimerButton = new JButton("Click to start sleep time");
        sleepTimerButton.addActionListener(this);
        sleepTimerButton.setSize(100,20);
        sleepTimerButton.setFont(new Font("Comic Sans", Font.PLAIN,20));
        sleepTimerButton.setForeground(new Color(0x00AF00));
        sleepTimerButton.setBackground(Color.black);
        sleepTimerButton.setOpaque(true);



        dayLabel = new JLabel();
        dayLabel.setFont(new Font("Ink Free", Font.BOLD, 40));

        dateLabel = new JLabel();
        dateLabel.setFont(new Font("Ink Free", Font.BOLD, 40));

        //timeLabel.setText(time);
        time = timeFormat.format(Calendar.getInstance().getTime());

        this.add(timeButton);
        this.add(sleepTimerButton);
        this.add(dayLabel);
        this.add(dateLabel);
        this.add(finishTimeLabel);
        this.setVisible(true);

        setTime();
     }

    public void setTime(){
        while(true){
            time = timeFormat.format(Calendar.getInstance().getTime());
            //timeLabel.setText(time);
            timeButton.setText(time);

            day = dayFormat.format(Calendar.getInstance().getTime());
            dayLabel.setText(day);

            date = dateFormat.format(Calendar.getInstance().getTime());
            dateLabel.setText(date);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    @Override
    public void actionPerformed(ActionEvent e) {

        if(e.getSource()== sleepTimerButton) {
            if(sleepTimerButton.getText() == "Click to start sleep time"){
                sleepTimerButton.setText("Click to stop sleep time");
                kiki = timeFormat.format(Calendar.getInstance().getTime());
                finishTimeLabel.setVisible(false);

            }
            else if(sleepTimerButton.getText() == "Click to stop sleep time"){
                sleepTimerButton.setText("Click to start sleep time");
                Date date1 = null;
                try {
                    date1 = timeFormat.parse(kiki);
                } catch (ParseException parseException) {
                    parseException.printStackTrace();
                }
                Date date2 = null;
                try {
                    date2 = finishTime.parse(timeFormat.format(Calendar.getInstance().getTime()));
                } catch (ParseException parseException) {
                    parseException.printStackTrace();
                }
                long sleptTime = date2.getTime() - date1.getTime();
                String hms = String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(sleptTime),
                        TimeUnit.MILLISECONDS.toMinutes(sleptTime) - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(sleptTime)),
                        TimeUnit.MILLISECONDS.toSeconds(sleptTime) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(sleptTime)));

                finishTimeLabel.setText(hms);
                finishTimeLabel.setVisible(true);
            }
        }
    }
}