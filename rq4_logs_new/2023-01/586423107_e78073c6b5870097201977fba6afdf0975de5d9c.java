import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.sql.*;

public class AddPostContent extends JFrame {
    Connection con = null;
    PreparedStatement pstmt = null;
    String driver = "com.mysql.cj.jdbc.Driver";
    String url = "jdbc:mysql://localhost/modeldb";
    String user = "root", pw = "1234";
    String SQL = "insert into post(post_id, content, image_id, user_id) values (?,?,?,?)";

    String entered = " "; // 게시글 내용
    JLabel imageLabel = new JLabel(); // 선택한 파일 이미지
    Toolkit toolkit = getToolkit(); // 이미지를 가져오기 위해서

    int i = 0;

    AddPostContent(String userid, String filePath, String fileName){
        // 전체 프레임
        JFrame frm = new JFrame("Add Post Content");
        frm.setSize(700,1000);
        frm.setLocationRelativeTo(null);
        frm.setResizable(false);
        Container contentPane = getContentPane();

        // 상단 ------------------------------------------------------------------
        JPanel upper = new JPanel();
        upper.setPreferredSize(new Dimension(700,400));
        imageLabel.setPreferredSize(new Dimension(350, 350));
        upper.setBackground(Color.WHITE);

        JLabel selectedlabel = new JLabel("Selected File");
        selectedlabel.setHorizontalAlignment(JLabel.CENTER); // 가운데 정렬
        selectedlabel.setPreferredSize(new Dimension(500, 20));
        upper.add(selectedlabel);

        Image image = toolkit.getImage(filePath);
        Image image2 = image.getScaledInstance(350, 350, Image.SCALE_SMOOTH); // 사이즈 조절
        ImageIcon icon = new ImageIcon(image2);
        imageLabel.setIcon(icon); // 이미지 라벨에 이미지 아이콘 추가
        upper.add(imageLabel); // upper 패널에 선택된 이미지 나오도록

        System.out.println("File Path: " + filePath); // 그냥 출력용
        System.out.println("File Name: " + fileName); // 그냥 출력용

        // 중앙 ------------------------------------------------------------------
        JPanel center = new JPanel();
        center.setLayout(new FlowLayout());
        center.setPreferredSize(new Dimension(700,410));
        center.setBackground(Color.WHITE);

        JLabel entercontent = new JLabel("Content");
        entercontent.setPreferredSize(new Dimension(600, 50));
        entercontent.setHorizontalAlignment(JLabel.CENTER);

        JTextArea textarea = new JTextArea("Enter content here", 15, 60);  // 게시물 내용 입력
        textarea.setBackground(Color.LIGHT_GRAY);

        center.add(entercontent);
        center.add(textarea);


        // 하단 ------------------------------------------------------------------
        JPanel bottom = new JPanel();
        bottom.setPreferredSize(new Dimension(700,190));
        bottom.setBackground(Color.WHITE);
        //bottom.setBackground(Color.GRAY);

        JButton uploadbtn = new JButton("Upload Post");
        uploadbtn.setPreferredSize(new Dimension(300, 100));
        uploadbtn.setBackground(Color.WHITE);
        uploadbtn.setHorizontalAlignment(JButton.CENTER);
        uploadbtn.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                entered = textarea.getText(); // textarea에 입력한 내용
                System.out.println("Content: " + entered);
                //String SQL = "INSERT into post values(?, ?, ?, ?)";
                try{
                    Class.forName(driver);

                    con = DriverManager.getConnection(url,user,pw);
                    pstmt = con.prepareStatement(SQL);

                    pstmt.setString(1, "3"); // 임시
                    pstmt.setString(2, entered);
                    pstmt.setString(3, fileName); // 임시
                    pstmt.setString(4, userid);
                    //쿼리 실행
                    System.out.println(pstmt);
                    int r = pstmt.executeUpdate();

                    i++;

                } catch(SQLException ex){
                    ex.printStackTrace();
                }catch(ClassNotFoundException ex){
                    ex.printStackTrace();
                }

                try{
                    if(con!=null && !con.isClosed())
                        con.close();
                }catch(SQLException ex){
                    ex.printStackTrace();
                }

                new MainInsta(userid);
                frm.setVisible(false);
            }
        });

        bottom.add(uploadbtn);


        // 전체 프레임에 추가
        contentPane.add(upper, BorderLayout.NORTH);
        contentPane.add(center, BorderLayout.CENTER);
        contentPane.add(bottom, BorderLayout.SOUTH);

        frm.add(contentPane);
        frm.setVisible(true);
        /*
        try{
            String dbURL = "jdbc:mysql://localhost/modeldb";
            String dbID = "root";
            String dbPW = "1234";

            Class.forName("com.mysql.cj.jdbc.Driver");

            conn = DriverManager.getConnection(dbURL, dbID, dbPW);

        } catch (Exception e) {
            e.printStackTrace();
        }

         */
    }


    public static void main(String[] args) {
        String initialPath = "";
        String initialName = "";
        //new AddPostContent(initialPath, initialName);
    }
}