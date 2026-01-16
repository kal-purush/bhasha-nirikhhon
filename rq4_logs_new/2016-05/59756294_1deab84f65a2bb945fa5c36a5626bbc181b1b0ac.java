import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import java.io.*;

public class HighScores extends JPanel
{
  final int MAX_HIGHSCORES = 10;
  String [][] highScores = new String[MAX_HIGHSCORES][3];
  public HighScores()
  {
    try
    {
      BufferedReader in = new BufferedReader(new FileReader ("Input.txt"));
      for(int x = 0;x<MAX_HIGHSCORES;x++)
      {
        String line = in.readLine();
        if(line == null)
        {
          highScores[x][0] = "";
          highScores[x][1] = "";
          highScores[x][2] = "";
        }
        else
          highScores[x] = line.split(" ");
      }
      for(int x = 0;x < MAX_HIGHSCORES;x++)
      {
        for(int y = 0;y < 3;y++)
        {
          System.out.print(highScores[x][y] + " ");
        }
        System.out.println();
      }
    }
    catch(IOException e)
    {}
  }
  public void update(String [] newScore) // already split
  {
    for(int x = 0;x < MAX_HIGHSCORES;x++)
    {
      if(highScores[x][1] == "")
      {
        highScores[x] = newScore;
        break;
      }
      else
        if(Integer.parseInt(newScore[1]) > Integer.parseInt(highScores [x][1]))
      {
        String [] temp = highScores[x];
        highScores[x] = newScore;
        newScore = temp;
      }
    }
    try
    {
      PrintWriter out = new PrintWriter(new FileWriter("Input.txt"));
    for(int x = 0;x < MAX_HIGHSCORES;x++)
    {
      for(int y = 0;y < 3;y++)
      {
        out.print(highScores[x][y] + " ");
      }
      out.println();
    }
    out.close();
    }
    catch(IOException e)
    {}
  }
  
  public void setUpHighScoresPanel()
  {
    GridBagLayout gbl = new GridBagLayout();
    GridBagConstraints gbc = new GridBagConstraints();
    
    setLayout(gbl);
    gbc.insets = new Insets(10,10,10,10);
    gbc.gridx=0;
    gbc.gridy=0;
    gbc.gridwidth = 3;
    gbc.anchor = gbc.CENTER;
    JLabel test = new JLabel("High Scores");
    add (test,gbc);
    gbc.gridwidth = 1;
    JLabel testName,testScore,testLevel;
    
    gbc.weighty = 1;
    gbc.anchor = gbc.LINE_START;
    gbc.gridy=1;
    gbc.gridx=0;
    testName = new JLabel("Name");
    add(testName,gbc);
    gbc.gridx=1;
    testScore = new JLabel("Score");
    add(testScore,gbc);gbc.gridx=0;
    gbc.gridx=2;
    testLevel = new JLabel("Level");
    add(testLevel,gbc);
    for(int x = 2; x<=11;x++)
    {
      gbc.gridx=0;
      gbc.gridy=x;
      testName = new JLabel((x-1) + ". " + highScores[x-2][0]);
      add(testName,gbc);
      gbc.gridx=1;
      testScore = new JLabel(highScores[x-2][1]);
      add(testScore,gbc);
      gbc.gridx=2;
      testLevel = new JLabel(highScores[x-2][2]);
      add(testLevel,gbc);
    }
  }
}