using System;
using System.Collections.Generic;
using System.Linq;
using System.ComponentModel;
using System.ComponentModel.Composition;

using Rock;
using Rock.Attribute;
using Rock.Data;
using Rock.Model;
using Rock.Workflow;
using Rock.Web.Cache;

namespace com.reallifeministries.RockExtensions.Workflow.Action
{
    /// <summary>
    /// Activates a new activity for a given activity type
    /// </summary>
    [Description("Get's a list of family members as a string for a given person object")]
    [Export(typeof(ActionComponent))]
    [ExportMetadata("ComponentName", "Get Family From Person")]

    [WorkflowAttribute("PersonAttribute", "The workflow attribute containing the person.", true, "", "", 0, null,
        new string[] { "Rock.Field.Types.PersonFieldType" })]
    public class GetFamilyFromPerson : ActionComponent
    {
        public override bool Execute(RockContext rockContext, WorkflowAction action, object entity, out List<string> errorMessages)
        {
            errorMessages = new List<string>();            
            var personAttribute = GetAttributeValue(action, "PersonAttribute");
            Guid personAttrGuid = personAttribute.AsGuid();

            if (!personAttrGuid.IsEmpty())
            {
                var personAttributeInst = AttributeCache.Read(personAttrGuid, rockContext);
                if (personAttributeInst != null)
                {
                    string attributePersonValue = action.GetWorklowAttributeValue(personAttrGuid);
                    Guid personAliasGuid = attributePersonValue.AsGuid();

                    if (personAliasGuid != null)
                    {
                        var personAlias = (new PersonAliasService(rockContext)).Get(personAliasGuid);
                        if (personAlias != null)
                        {
                            var familyNames = personAlias.Person.GetFamilyMembers().Select(f => f.Person.FirstName);
                            action.Activity.Workflow.SetAttributeValue("FamilyNames", String.Join(", ", familyNames));
                        }
                        else
                        {
                            errorMessages.Add(string.Format("PersonAlias cannot be found: {0}", personAliasGuid));
                        }
                    }

                }
                else
                {
                    errorMessages.Add(string.Format("Person attribute could not be found for '{0}'!", personAttrGuid.ToString()));
                }
            }
            else
            {
                errorMessages.Add(string.Format("Selected person attribute ('{0}') was not a valid Guid!", personAttribute));
            }

            return true;
        }
    }
}