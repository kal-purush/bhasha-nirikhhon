/*using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Player : AI
{
    public override bool DecideAddKan(List<int> hand, List<CallCardsSet> call)
    {
        return false;
    }

    public override bool DecideClosedKan(List<int> hand)
    {
        return false;
    }

    public override void DecideDiscard(List<int> indexOnly)
    {
        discardOrKan_index = indexOnly[0];
    }

    public override bool DecideOpenKan(List<int> hand)
    {
        return PL_OpenKan;
    }

    public override bool DecidePon(List<int> hand, ref bool bonus)
    {
        return PL_Pon;
    }

    public override bool DecideTi(List<int> hand, ref int[] indexes, int discard_other)
    {
        return PL_Ti;
    }
}
*/