package com.myscrabble.uicomponents;

import java.awt.Font;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.lwjgl.opengl.GL11;
import org.newdawn.slick.Color;
import org.newdawn.slick.TrueTypeFont;
import org.newdawn.slick.opengl.Texture;

import com.myscrabble.entities.Player;
import com.myscrabble.managers.ResourceManager;
import com.myscrabble.util.RenderUtils;

/**
 * 
 * @author Alex Koukoulas 2074245k
 * Class Description:
 * A class handling the score display 
 */
public class ScoreDisplay 
{
    private static final String TEX_DIR = "/misc/score/";
    private static final int DEFAULT_STYLE = 1;
    
    private static final float TEXTURE_MARGIN = 40.0f;
    private static final float[] renderingPos1 = new float[]{584.0f, 66.0f};
    private static final float[] renderingPos2 = new float[]{584.0f, 140.0f};
    
    private ResourceManager rm;
    private HashMap<Player, Integer> playerPoints;
    private ArrayList<Texture> scoreTextures;
    
    private char[] player1Comps;
    private char[] player2Comps;
    
    private int style;
    private TrueTypeFont font;
    
    public ScoreDisplay(ResourceManager rm)
    {
        this.rm = rm;
        style = DEFAULT_STYLE;
        scoreTextures = rm.getAllTextures(TEX_DIR + style);
        
        Font awtFont = new Font("Times New Roman", Font.BOLD, 24);
        font = new TrueTypeFont(awtFont, false);
    }
    
    public void update()
    {
        
    }
    
    public void render()
    {
        renderPlayerScore();
        
        GL11.glPushAttrib(GL11.GL_CURRENT_BIT);
        font.drawString(200, 200, "ALEX", Color.cyan);
        GL11.glPopAttrib();
    }
    
    private void renderPlayerScore()
    {
        for(Entry<Player, Integer> entry : playerPoints.entrySet())
        {
            char[] scoreComps = getComponents(entry.getValue());
            
            float[] renderingPos = entry.getKey().getName().equals("Player 1") ? renderingPos1 : renderingPos2;
            
            for(int i = 0; i < scoreComps.length; i++)
            {
                Texture currentTexture = scoreTextures.get(Integer.parseInt(String.valueOf(scoreComps[i]))); 
                RenderUtils.renderTexture(currentTexture, renderingPos[0] + i * TEXTURE_MARGIN, renderingPos[1]);
            }
        
        }
    }
    
    private char[] getComponents(Integer score)
    {
        char[] result = new char[3];
        
        if(score < 10)
        {
            result[0] = '0';
            result[1] = '0';
            result[2] = String.valueOf(score).charAt(0);
        }
        else if(score < 100)
        {
            result[0] = '0';
            result[1] = String.valueOf(score).charAt(0);
            result[2] = String.valueOf(score).charAt(1);
        }
        else
        {
            result = String.valueOf(score).toCharArray();
        }
        
        return result;
    }
    
    public void setPlayerPoints(HashMap<Player, Integer> playerPoints)
    {
        this.playerPoints = playerPoints;
    }    
}