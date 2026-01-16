package vueControleur.vues.components;

import vueControleur.icon.IconNames;
import vueControleur.icon.IconRepository;

import javax.swing.*;
import java.awt.*;
import java.awt.image.BufferedImage;

import static vueControleur.icon.IconNames.VIDE;

public class Case extends JPanel {
    private final JLabel label;
    private final IconRepository icones = IconRepository.getInstance();


    public Case() {
        super();
        this.label = new JLabel();
        setBackground(Color.GREEN);
        label.setHorizontalAlignment(SwingConstants.CENTER);
        label.setVerticalAlignment(SwingConstants.CENTER);
        setIcon(VIDE);
        add(label);
    }

    public void setIcon(IconNames iconNames) {
        ImageIcon icon = icones.getIcone(iconNames);
        label.setIcon(icon);
    }

    public void setIconLegume(IconNames iconName, float legumeVie, float legumeCroissance) {
        ImageIcon icon = icones.getIcone(iconName);
        setIconLegume(icon, legumeVie, legumeCroissance);
    }

    public void setIconLegume(ImageIcon icon, float legumeVie, float legumeCroissance) {
        ImageIcon tintedIcon = getTintedIcon(icon, legumeVie);
        ImageIcon scaledIcon = getScaledIcon(tintedIcon, legumeCroissance);
        label.setIcon(scaledIcon);
    }

    private ImageIcon getScaledIcon(ImageIcon tintedIcon, float legumeCroissance) {
        float minSize = 0.5f;
        if (legumeCroissance == 1) {
            return tintedIcon;
        } else if (legumeCroissance < minSize) {
            legumeCroissance = minSize;
        }
        int width = tintedIcon.getIconWidth();
        int height = tintedIcon.getIconHeight();
        int newWidth = (int) (width * legumeCroissance);
        int newHeight = (int) (height * legumeCroissance);
        Image image = tintedIcon.getImage().getScaledInstance(newWidth, newHeight, Image.SCALE_SMOOTH);
        return new ImageIcon(image);
    }

    private ImageIcon getTintedIcon(ImageIcon icon, float legumeVie) {
        float tintValue = 1 - legumeVie;
        Color tintColor = new Color(1, 0, 0, tintValue);
        return applyTintColor(icon, tintColor);
    }

    private ImageIcon applyTintColor(ImageIcon icon, Color tintColor) {
        BufferedImage tintedImage = new BufferedImage(icon.getIconWidth(), icon.getIconHeight(), BufferedImage.TYPE_INT_ARGB);
        Graphics2D g = tintedImage.createGraphics();
        g.setComposite(AlphaComposite.Src);
        g.drawImage(icon.getImage(), 0, 0, null);
        g.setComposite(AlphaComposite.SrcAtop);
        g.setColor(tintColor);
        g.fillRect(0, 0, icon.getIconWidth(), icon.getIconHeight());
        g.dispose();
        return new ImageIcon(tintedImage);
    }

}