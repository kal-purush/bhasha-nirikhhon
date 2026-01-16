import javax.swing.JFrame;
import javax.swing.ImageIcon;
import javax.swing.text.html.ImageView;

import java.awt.*;
import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class PlayerSelectionPage extends JFrame{
    private JPanel          jp;
    private ImageIcon       imgIcon;
    private ImageIcon       Unselect;
    private ImageIcon       Select;
    private JButton         ImgBtn;
    private PlayerSelectionPage     screen;
    private Image           GetImage;
    public void Init(){
        screen = new PlayerSelectionPage();
        screen.setTitle("HomePage");
        screen.setVisible(true);
        screen.setSize(900, 630);
        screen.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        // First Page
        PlayerSelectionPage();
        screen.validate();
    }

    public void ButtonImageCreate(String UnSelectURL,String SelectedURL,int x , int y, int w , int h,int MouseOver){
        // System.out.println("In");
        jp.setLayout(null);
        ImgScaled(UnSelectURL,w,h);
        ImgBtn = new JButton(imgIcon);
        BtnFormat(ImgBtn);
        BtnPosition(ImgBtn,x,y,w,h);
        if(MouseOver==1)
            BtnMouseOver(ImgBtn, SelectedURL,UnSelectURL, w, h);
        jp.add(ImgBtn);
    }

    public void ImgScaled(String URL ,int w ,int h){
        imgIcon = new ImageIcon(URL);
        GetImage = imgIcon.getImage();
        GetImage = GetImage.getScaledInstance(w,h, java.awt.Image.SCALE_SMOOTH);
        imgIcon = new ImageIcon(GetImage);
    }

    public void BtnFormat(JButton ImgBtn){
        ImgBtn.setContentAreaFilled(false); // Remove Btn Background
        ImgBtn.setFocusable(false);
        ImgBtn.setBorderPainted(false); // Remove Btn Border
    }
    public void BtnPosition(JButton ImgBtn ,int x , int y, int w , int h){
        // System.out.println("SetBounds");
        ImgBtn.setBounds(x,y,w,h);
    }

    public void BtnMouseOver(JButton ImgBtn,String SelectedURL, String UnSelectURL,int w , int h){

        ImgBtn.addMouseListener(new java.awt.event.MouseAdapter() {

            public void mouseEntered(java.awt.event.MouseEvent evt) {
                // System.out.println("ButtonMouseOver");
                ImgScaled(SelectedURL, w, h);
                ImgBtn.setIcon(imgIcon);
            }
            public void mouseExited(java.awt.event.MouseEvent evt) {
                ImgScaled(UnSelectURL, w, h);
                ImgBtn.setIcon(imgIcon);
                
            }
        });
    }

    public void BtnClick(){

    }
    public void PlayerSelectionPage(){
        jp = new JPanel();
        jp.setBackground(Color.GRAY);
        ButtonImageCreate("Img/Player1-1UnSelected.png","Img/Player1-1.png",60, 150, 170, 230,1);
        ButtonImageCreate("Img/Player1-2UnSelected.png","Img/Player1-2.png",360, 150, 170, 230,1);
        ButtonImageCreate("Img/Player1-3UnSelected.png","Img/Player1-3.png",660, 150, 170, 230,1);
        ButtonImageCreate("Img/OKBTN-UnSelected.png","Img/OKBTN.png",355, 450,150, 70,0);

        screen.add(jp);

    }
    public static void main(String[] args) {
        PlayerSelectionPage GUI = new PlayerSelectionPage();
        GUI.Init();
    }
}