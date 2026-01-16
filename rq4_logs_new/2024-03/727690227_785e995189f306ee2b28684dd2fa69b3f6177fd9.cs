using System;
using System.Collections.Generic;
using UnityEngine;
using Visualisation.Animation;

namespace Visualization.ClassDiagram
{

public class ClassObserver : Observer
{
        public ClassObserver(HighlightSubject subject) : base(subject)
        {
        }

        public override void Update()
    {
        ClassHighlightSubject classHighlightSubject = Subject as ClassHighlightSubject;
        if (classHighlightSubject.HighlightInt == 1) {
            Animation.Animation.Instance.HighlightClass(classHighlightSubject.ClassName,true);
            Animation.Animation.Instance.HighlightEdge(classHighlightSubject.InvocationInfo.Relation?.RelationshipName, true, classHighlightSubject.InvocationInfo);
        }
        else if (classHighlightSubject.HighlightInt == 0) {
            Animation.Animation.Instance.HighlightClass(classHighlightSubject.ClassName, false);
            Animation.Animation.Instance.HighlightEdge(classHighlightSubject.InvocationInfo.Relation?.RelationshipName, false, classHighlightSubject.InvocationInfo);
        }
        
    }

}



}