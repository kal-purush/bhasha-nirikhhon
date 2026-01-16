package com.softpos.main.program;

import database.MySQLConnect;
import java.awt.Color;
import java.awt.Font;
import java.awt.Image;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.swing.BorderFactory;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.SwingConstants;
import util.MSG;

/**
 *
 * @author nathee
 */
public class ButtonCustom {

    public static JButton getButtonLayout(String menuCode, int menuIndex) {
        MenuMGR m = new MenuMGR();
        int fontType = Font.PLAIN;
        int layout = 0;

        MySQLConnect mysql = new MySQLConnect();
        mysql.open();
        try {
            String sql = "select * from soft_menusetup "
                    + "where MenuCode='" + menuCode + "' "
                    + "and M_Index='" + menuIndex + "'";
            Statement stmt = mysql.getConnection().createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                m.setMenuCode(rs.getString("MenuCode"));
                m.setMenuType(rs.getInt("MenuType"));
                m.setPCode(rs.getString("PCode"));
                m.setMenuShowText(sun.natee.project.util.ThaiUtil.ASCII2Unicode(rs.getString("MenuShowText")));
                m.setIMG(rs.getString("IMG"));
                m.setFontColor(rs.getString("FontColor"));
                m.setBGColor(rs.getString("BGColor"));
                m.setLayout(rs.getInt("Layout"));
                m.setFontSize(rs.getInt("FontSize"));
                m.setFontName(rs.getString("FontName"));
                m.setMIndex(rs.getInt("M_Index"));
                m.setFontAttr(rs.getString("FontAttr"));
                m.setImgSize(rs.getInt("IMG_SIZE"));
                layout = m.getLayout();

                switch (m.getFontAttr()) {
                    case "B":
                        fontType = Font.BOLD;
                        break;
                    case "I":
                        fontType = Font.ITALIC;
                        break;
                    case "BI":
                        fontType = Font.BOLD | Font.ITALIC;
                        break;
                    default:
                        break;
                }
            }

            rs.close();
            stmt.close();
        } catch (SQLException e) {
            MSG.ERR(e.getMessage());
        } finally {
            mysql.close();
        }

        JButton button = new JButton("");
        button.setName(menuCode);
        if (m.getIMG() == null) {
            m.setIMG("");
        }
        if (layout == 0) {
            button.setFont(new Font(m.getFontName(), fontType, m.getFontSize()));
            if (!m.getIMG().equals("")) {
                updateIconFull(button, m.getIMG(), m.getImgSize());
            }
            button.setText("<html>" + m.getMenuShowText() + "</html>");
            button.setHorizontalTextPosition(SwingConstants.CENTER);
            button.setVerticalAlignment(SwingConstants.TOP);
            button.setVerticalTextPosition(SwingConstants.BOTTOM);
            button.setForeground(getColorFormat(m.getFontColor()));
        } else if (layout == 1) {
            button.setFont(new Font(m.getFontName(), fontType, m.getFontSize()));
            if (!m.getIMG().equals("")) {
                updateIconFull(button, m.getIMG(), m.getImgSize());
            }
            button.setText("<html>" + m.getMenuShowText() + "</html>");
            button.setHorizontalTextPosition(SwingConstants.CENTER);
            button.setVerticalAlignment(SwingConstants.BOTTOM);
            button.setVerticalTextPosition(SwingConstants.TOP);
            button.setForeground(getColorFormat(m.getFontColor()));
        } else if (layout == 2) {
            button.setFont(new Font(m.getFontName(), fontType, m.getFontSize()));
            button.setForeground(getColorFormat(m.getFontColor()));
            if (!m.getIMG().equals("")) {
                updateIconFull(button, m.getIMG(), m.getImgSize());
            }
            button.setText("<html>" + m.getMenuShowText() + "</html>");
            button.setHorizontalTextPosition(SwingConstants.CENTER);
        } else if (layout == 3) {
            button.setFont(new Font(m.getFontName(), fontType, m.getFontSize()));
            button.setForeground(getColorFormat(m.getFontColor()));
            button.setText("<html><center>" + m.getMenuShowText() + "</center></html>");
            button.setHorizontalTextPosition(SwingConstants.CENTER);

            if (!m.getIMG().equals("")) {
                updateIconFull(button, m.getIMG(), m.getImgSize());
            }
        } else if (layout == 4) {
            button.setBackground(getColorFormat(m.getBGColor()));
            button.setFont(new Font(m.getFontName(), fontType, m.getFontSize()));
            button.setForeground(getColorFormat(m.getFontColor()));
            button.setText("<html>" + m.getMenuShowText() + "</html>");
            button.setHorizontalTextPosition(SwingConstants.CENTER);
        } else if (layout == 5) {
            button.setFont(new Font(m.getFontName(), fontType, m.getFontSize()));
            if (!m.getIMG().equals("")) {
                updateIconFull(button, m.getIMG(), m.getImgSize());
            }
            button.setHorizontalTextPosition(SwingConstants.CENTER);
            button.setVerticalTextPosition(SwingConstants.BOTTOM);
        } else if (layout == 6) {
            button.setBackground(getColorFormat(m.getBGColor()));
            button.setFont(new Font(m.getFontName(), fontType, m.getFontSize()));
            if (!m.getIMG().equals("")) {
                updateIconFull(button, m.getIMG(), m.getImgSize());
            }
            button.setText("<html>" + m.getMenuShowText() + "</html>");
            button.setHorizontalTextPosition(SwingConstants.CENTER);
            button.setVerticalAlignment(SwingConstants.TOP);
            button.setVerticalTextPosition(SwingConstants.BOTTOM);
            button.setForeground(getColorFormat(m.getFontColor()));
        }

        //check empty text name
        if (button.getText().trim().equals("<html>null</html>")) {
            button.setOpaque(false);
            button.setFocusPainted(false);
            button.setBorderPainted(false);
            button.setContentAreaFilled(false);
            button.setBorder(BorderFactory.createEmptyBorder(0, 0, 0, 0));
        }

        return button;
    }

    private void updateIconDefault(JButton button, String img) {
        ImageIcon icon = new ImageIcon(img);
        int scale = 1; // 2 times smaller
        int width = button.getWidth() - 35;//icon.getIconWidth();
        int newWidth = width / scale;
        button.setIcon(new ImageIcon(icon.getImage().getScaledInstance(newWidth, -1, Image.SCALE_SMOOTH)));
    }

    static void updateIconFull(JButton button, String img, int imgSize) {
        //full button image
        ImageIcon icon = new ImageIcon(img);
        int scale = 1;
        int width = imgSize;//icon.getIconWidth();
        if (width <= 0) {
            width = 125;
        }
        int newWidth = width / scale;
        button.setIcon(new ImageIcon(icon.getImage().getScaledInstance(newWidth, -1, Image.SCALE_SMOOTH)));
    }

    public static Color getColorFormat(String bgColor) {
        int red = 240, green = 240, blue = 240;
        if (bgColor != null) {
            String[] color = bgColor.split(",");
            if (color.length == 3) {
                try {
                    red = Integer.parseInt(color[0]);
                    green = Integer.parseInt(color[1]);
                    blue = Integer.parseInt(color[2]);
                } catch (NumberFormatException e) {
                    System.out.println(e.getMessage());
                }
            }
        }

        return new Color(red, green, blue);
    }
}