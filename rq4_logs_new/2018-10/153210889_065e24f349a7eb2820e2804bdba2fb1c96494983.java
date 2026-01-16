import greenfoot.*;
import java.util.*;

public class FishingLine extends GameObject
{
    private static final double MinLength = 0;
    private static final double MaxLength = 400;

    private Hook _hook;
    private double _length;

    public FishingLine(Hook hook)
    {
        _hook = hook;
    }

    public double getLength()
    {
        return _length;
    }
    
    public List<Fish> takeFish()
    {
        return _hook.takeFish();
    }

    public void update()
    {
        _hook.setPosition(getPosition().add(getOffset(new Vector2(0, -_length))));
        _hook.setRotation(getRotation());
    }

    public void extend(double amount)
    {
        _length += amount;
        _length = Math.min(_length, MaxLength);
        _length = Math.max(_length, MinLength);
    }

    public void draw(GreenfootImage canvas, Camera camera)
    {
        Vector2 start = camera.getScreenPosition(getPosition());
        Vector2 end = camera.getScreenPosition(_hook.getPosition());
        canvas.setColor(Color.WHITE);
        canvas.drawLine(
            (int) start.getX(), 
            (int) start.getY(), 
            (int) end.getX(), 
            (int) end.getY());
    }
}