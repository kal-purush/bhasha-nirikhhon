using System;
using System.Collections.Generic;
using OALProgramControl;
using UnityEngine;

namespace Visualization.ClassDiagram
{

public class HighlightFill : HighlightEdgeState
{
    protected static HighlightFill instance = null;
    public static HighlightFill GetInstance() {
        if (instance == null) {
            instance = new HighlightFill();
        }
        return instance;
    }
    public override void Highligt(MethodInvocationInfo Call) {
        Animation.Animation a = Animation.Animation.Instance;
        a.RunAnimateFill(Call);
    }

}



}