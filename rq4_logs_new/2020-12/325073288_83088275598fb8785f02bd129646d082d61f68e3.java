import greenfoot.*;  

//***image citation***
//"Free Image on Pixabay - Cartoon, Background, Day, Nature.” 
//pixabay, https://pixabay.com/en/cartoon-background-day-nature-2614618/
//*********************

public class Background extends Actor
{
    // make background scroll
    public void scroll()
    {
        if (getX() <= -getImage().getWidth()/2) 
        {
            setLocation(getWorld().getWidth()+500, getY());
        }
        setLocation(getX()-5, getY());
    }
}