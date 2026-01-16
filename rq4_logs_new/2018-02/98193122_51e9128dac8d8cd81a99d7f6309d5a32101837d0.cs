using PaderbornUniversity.SILab.Hip.UserStore.Model.Rest;

using PaderbornUniversity.SILab.Hip.UserStore.Model.Rest.Actions;

using System.Collections.Generic;



namespace PaderbornUniversity.SILab.Hip.UserStore.Model.Entity.Actions

{

    public class ExhibitVisitedAction : Action
    {
        public ExhibitVisitedAction(ExhibitVisitedActionArgs args) : base(args)
        {

        }

        public override string TypeName => ActionTypes.ExhibitVisited.Name;

        public override ActionResult CreateActionResult()
        {
            return new ExhibitVisitedActionResult(this);
        }

        public static List<Action> Factory(ExhibitVisitedActionsArgs args)
        {
            var result = new List<Action>();
            foreach (var actionArg in args.ToListActionArgs())
            {
                result.Add(new ExhibitVisitedAction((ExhibitVisitedActionArgs)actionArg));
            }
            return result;
        }

        public override ActionArgs CreateActionArgs()
        {
            return new ExhibitVisitedActionArgs();
        }
    }
}