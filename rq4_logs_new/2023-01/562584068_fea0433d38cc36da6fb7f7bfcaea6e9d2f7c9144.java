package de.hebk.gui;

import javax.swing.*;
import java.awt.*;
import java.util.Objects;

public class JImagePanel extends JPanel {
    // I don't know what this class exacly does but it works

    private final Image image;

    private boolean scale;

    public JImagePanel(Image anImage) {
        image = Objects.requireNonNull(anImage);
    }

    public JImagePanel(Image anImage, LayoutManager aLayout) {
        super(aLayout);
        image = Objects.requireNonNull(anImage);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void paintComponent(Graphics g) {
        super.paintComponent(g);
        final Image toDraw = scale ? image.getScaledInstance(getWidth(), getHeight(), Image.SCALE_SMOOTH) : image;
        g.drawImage(toDraw, 0, 0, this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Dimension getPreferredSize() {
        if (isPreferredSizeSet()) {
            return super.getPreferredSize();
        } else {
            return new Dimension(image.getWidth(this), image.getHeight(this));
        }
    }

    public boolean isScale() {
        return scale;
    }

    public void setScale(boolean scale) {
        this.scale = scale;
    }
}