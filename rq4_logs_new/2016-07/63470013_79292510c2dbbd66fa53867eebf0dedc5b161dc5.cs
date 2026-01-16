using System;

namespace Paynter.WitAi.Actions
{
    [System.AttributeUsage(System.AttributeTargets.Method)]
    public class WitAiAction: Attribute
    {
        public string ActionName { get; private set; }
        
        public WitAiAction()
        {}

        public WitAiAction(string actionName)
        {
            ActionName = actionName;
        }
    }
}