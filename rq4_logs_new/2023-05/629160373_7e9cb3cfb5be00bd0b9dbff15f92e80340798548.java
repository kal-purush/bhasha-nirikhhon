import javax.swing.*;
import javax.swing.plaf.TableHeaderUI;
import java.awt.*;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.util.Random;

public class Captcha extends JFrame implements Runnable , KeyListener {
    JPanel panel;
    JLabel label;
    Thread thread;
    StringBuilder typedNumber;
    int digit = 0;
    Random random;
    int randNumber;
    int FPS = 60;
    boolean k1,k2,k3,k4,k5,k6,k7,k8,k9,k10;


    public Captcha(){

        random = new Random();
        randNumber = random.nextInt(9000) + 1000;
        System.out.println(randNumber);

        typedNumber = new StringBuilder();

        label = new JLabel();
        label.setText(String.valueOf(randNumber));
        label.setFont(new Font("Arial",Font.BOLD,25));
        label.setBounds(100,100,500,100);


        panel = new JPanel(true);
        panel.addKeyListener(this);
        panel.setBounds(0,0,800,600);
        panel.setLayout(null);
        panel.setBackground(Color.WHITE);
        panel.add(label);


        this.add(panel);
        startThread();
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        addKeyListener(this);
        setLocationRelativeTo(null);
        setLayout(null);
        setSize(800,600);
        setResizable(false);
        setVisible(true);
    }

    public void startThread(){
        thread = new Thread(this,"Thread");
        thread.start();
    }



    @Override
    public void run() {

        double interval = 1000000000/FPS;
        double nextTime = (System.nanoTime()+interval);
        double lastTime = System.nanoTime();
        long currentTime ;
        int drawFPS = 0 ;
        long timer = 0;


        while (thread!=null) {

            update();

            try {
                double remainingTime = nextTime - System.nanoTime();
                remainingTime = remainingTime/1000000;

                if (remainingTime<0){
                    remainingTime=0;
                }

                Thread.sleep((long) remainingTime);
                nextTime += interval;

                currentTime = System.nanoTime();
                timer += currentTime-lastTime;
                lastTime = currentTime;
                drawFPS++;

                if (timer >= 1000000000){
                    System.out.println("FPS : "+drawFPS);
                    drawFPS=0;
                    timer=0;
                }

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

    }

    public void update(){
        if (k1==true){
            typedNumber.append(digit);
        }
        if (k2==true){
            typedNumber.append(digit);
        }
        if (k3==true){
            typedNumber.append(digit);
        }
        if (k4==true){
            typedNumber.append(digit);
        }
        if (k5==true){
            typedNumber.append(digit);
        }
        if (k6==true){
            typedNumber.append(digit);
        }
        if (k7==true){
            typedNumber.append(digit);
        }
        if (k8==true){
            typedNumber.append(digit);
        }
        if (k9==true){
            typedNumber.append(digit);
        }
        if (k10==true){
            typedNumber.append(digit);
        }

    }

    @Override
    public void keyTyped(KeyEvent e) {

        int c = e.getKeyCode();
        if (c==KeyEvent.VK_0){
            k1=true;
            digit=0;
            System.out.println(digit);
        }
        if (c==KeyEvent.VK_1){
            k2=true;
            digit=1;
        }
        if (c==KeyEvent.VK_2){
            k3=true;
            digit=2;
        }
        if (c==KeyEvent.VK_3){
            k4=true;
            digit=3;
        }
        if (c==KeyEvent.VK_4){
            k5=true;
            digit=4;
        }
        if (c==KeyEvent.VK_5){
            k6=true;
            digit=5;
        }
        if (c==KeyEvent.VK_6){
            k7=true;
            digit=6;
        }
        if (c==KeyEvent.VK_7){
            k8=true;
            digit=7;
        }
        if (c==KeyEvent.VK_8){
            k9=true;
            digit=8;
        }
        if (c==KeyEvent.VK_9){
            k10=true;
            digit=9;
        }


    }

    @Override
    public void keyPressed(KeyEvent e) {

    }

    @Override
    public void keyReleased(KeyEvent e) {

    }
    public static void main(String[] args) {
        new Captcha();
    }
}