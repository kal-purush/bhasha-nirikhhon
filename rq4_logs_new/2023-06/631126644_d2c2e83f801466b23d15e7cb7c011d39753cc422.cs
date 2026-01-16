using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public abstract class PieceBehavior
{
    /// <summary>
    /// The validateMove method checks to see if the given move is valid or not.
    /// </summary>
    /// <returns>bool indicating the validity of the move.</returns>
    public virtual bool validateMove(int startIndex, int endIndex)
    {
        // Return false by default
        Debug.LogError("Error: The requested piece has no defined valid moves!");
        return false;
    }
}