using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.ComponentModel;
using System.ComponentModel.Composition;

using Rock;
using Rock.Attribute;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;
using Rock.Workflow;

namespace com.reallifeministries.RockExtensions.Workflow.Action
{
    /// <summary>
    /// Generates a new attendence code for a given type
    /// </summary>
    [Description( "Generates a new attendence code for a given type" )]
    [Export( typeof( ActionComponent ) )]
    [ExportMetadata( "ComponentName", "Activate Workflow" )]
        
    [TextField("ServiceName", "Service to generate the code for",true)]
    [TextField("GeneratedCode", "Code that is generated from this action", true)]
    public class GenerateAttendenceCode : ActionComponent
    {
        /// <summary>
        /// Executes the specified workflow.
        /// </summary>
        /// <param name="rockContext">The rock context.</param>
        /// <param name="action">The action.</param>
        /// <param name="entity">The entity.</param>
        /// <param name="errorMessages">The error messages.</param>
        /// <returns></returns>
        public override bool Execute( RockContext rockContext, WorkflowAction action, Object entity, out List<string> errorMessages )
        {
            errorMessages = new List<string>();
            var globalAttributes = Rock.Web.Cache.GlobalAttributesCache.Read();
                        
            var serviceName = GetAttributeValue(action, "ServiceName").ResolveMergeFields(GetMergeFields(action));                                

            if (String.IsNullOrEmpty(serviceName))
            {
                action.AddLogEntry("Invalid ServiceName Property", true);
                return false;
            }
            AttendenceCode attendenceCode;
            if (Enum.TryParse<AttendenceCode>(serviceName.ToUpper(), out attendenceCode))
            {
                var generatedCode = GenerateCode(attendenceCode);
                if (generatedCode == null)
                {
                    action.AddLogEntry("GenerateCode return null, please check your serviceName", true);
                    return false;
                }
                globalAttributes.SetValue(String.Format("{0}AttendenceCode", serviceName.ToUpper()), generatedCode, true);
            }
            else
            {
                action.AddLogEntry("Attendence Code invalid, please check your serviceName", true);
                return false;
            }            
            return true;
        }

        private string GenerateCode(AttendenceCode attendenceCode)
        {
            // validate this service matches
            var prefix = "";
            switch (attendenceCode)
            {
                case AttendenceCode.PF :
                case AttendenceCode.CDA:
                    {
                        prefix = "lifer";
                    }
                    break;
                case AttendenceCode.THIRST:
                    {
                        prefix = "thirst";
                    }
                    break;
                case AttendenceCode.RECOVERY:
                    {
                        prefix = "recovery";

                    }
                    break;
                default:
                    {
                        prefix = null;
                    }
                    break;
            }
            Random r = new Random();
            int code = r.Next(1000, 9999);
            return prefix + code;
        }        
    }
}