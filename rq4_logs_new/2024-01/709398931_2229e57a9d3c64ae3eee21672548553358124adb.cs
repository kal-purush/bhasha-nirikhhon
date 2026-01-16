using uml.actions;
using uml.packages;

namespace fuml.semantics.actions
{
    public partial class CallOperationActionActivation : CallActionActivation
    {
        public bool IsExplicitBaseClassCall(CallOperationAction callOperationAction)
        {
            List<Stereotype> appliedStereotypes = callOperationAction.appliedStereotype;
            int i = 0;
            bool isExplicitBaseClassCall = false;
            while (i < appliedStereotypes.Count && !isExplicitBaseClassCall)
            {
                Stereotype s = appliedStereotypes.ElementAt(i);
                if (s.name.Equals("ExplicitBaseClassCall"))
                {
                    isExplicitBaseClassCall = true;
                }
            }
            return isExplicitBaseClassCall;
        }
    } // CallOperationActionActivation
}