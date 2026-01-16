using System.Collections;
using System.Collections.Generic;
using UnityEngine;

/*
Name: Alec Reyerson
ID: 1826582
Email: reyer101@mail.chapman.edu
Course: CPSC-340-01
Assignment: Semester Project

Description: This is a script representing the player character and their current state
*/

// PlayerCharacter
public class PlayerCharacter : MonoBehaviour {
    public int hp, stamina, strength, defense, intuition, experience, level;
    private int currHP, currStamina;

    // Awake
    void Awake()
    {
        currHP = hp;
        currStamina = stamina;        
    }
   
    // levelUp
    private void levelUp()
    {
        level++;
    }

   /*
   Name: increaseStrength
   Parameters: int increase
   */
    public void increaseStrength(int increase)
    {
        strength +=increase;
    }

   /*
   Name: increaseDefense
   Parameters: int increase
   */
    public void inreaseDefense(int increase)
    {
        strength += increase;
    }

    /*
   Name: increaseIntuition
   Parameters: int increase
   */
    public void inreaseIntuition(int increase)
    {
        intuition += increase;
    }

    /*
    Name: gainExperience
    Parameters: int experience
    */
    public void gainExperience(int experience)
    {
        experience += experience;
        if (Constants.LevelMap[level + 1] <= experience)
        {
            levelUp();
        }
    }

    // restoreHPAndStamina
    public void restoreHPAndStamina()
    {
        currHP = hp;
        currStamina = stamina;
    }

   /*
   Name: takeDamage
   Parameters: int damage
   */
    public void takeDamage(int damage)
    {
        if(currHP - damage > 0)
        {
            currHP -= damage;
            return;            
        }
        currHP = 0;
        // Player dead at this point so do stuff
    }

  /*
  Name: restoreHealth
  Parameters: int gained
  */
    public void gainHealth(int gained)
    {
        if(currHP + gained <= hp)
        {
            currHP += gained;
            return;
        }
        currHP = hp;
    }

   /*
   Name: spendStamina
   Parameters: int spent
   */
    public void spendStamina(int spent)
    {
        if(stamina - spent >= 0)
        {
            currStamina -= spent;
            return;
        }
        currStamina = 0;   
    }

   /*
   Name: spendStamina
   Parameters: int spent
   */
   public void gainStamina(int gained)
    {
        if(stamina + gained <= stamina)
        {
            currStamina += gained;
            return;
        }
        currStamina = stamina;
    }
}