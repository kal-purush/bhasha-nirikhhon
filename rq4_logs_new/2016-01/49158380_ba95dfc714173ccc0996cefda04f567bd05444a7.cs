using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.Composition;
using System.Linq;
using System.Data.Entity;

using Rock;
using Rock.Attribute;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;
using Rock.Workflow;

namespace com.reallifeministries.RockExtensions.Workflow.Action
{
    /// <summary>
    /// Posts attendance to a group
    /// </summary>
    [Description( "Set's attendance in a group." )]
    [Export( typeof( ActionComponent ) )]
    [ExportMetadata( "ComponentName", "Check In Person To Group" )]

    [WorkflowAttribute("Group", "The attribute containing the group to get the leader for.", true, "", "", 0, null,
        new string[] { "Rock.Field.Types.GroupFieldType", "Rock.Field.Types.GroupFieldType" })]

    [WorkflowAttribute("Person", "The attribute to set to the person in the group.", true, "", "", 1, null,
        new string[] { "Rock.Field.Types.PersonFieldType" })]

    [WorkflowAttribute("Attendance Date/Time", "The attribute with the date time to use for the attendance. Leave blank to use the current date time.", false, "", "", 2, "AttendanceDatetime")]

    [WorkflowAttribute("Campus", "The attribute to set to the Campus of the attendance ", true, "", "", 3, null,
        new string[] { "Rock.Field.Types.CampusFieldType" })]    
    public class CheckInPersonToGroup : ActionComponent
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

            Guid? groupGuid = null;
            Person person = null;
            DateTime attendanceDateTime = DateTime.MinValue;
            bool addToGroup = true;
           
            // get the group attribute
            Guid groupAttributeGuid = GetAttributeValue(action, "Group").AsGuid();

            if ( !groupAttributeGuid.IsEmpty() )
            {
                groupGuid = action.GetWorklowAttributeValue(groupAttributeGuid).AsGuidOrNull();

                if ( !groupGuid.HasValue )
                {
                    errorMessages.Add("The group could not be found!");
                }
            }

            // get person alias guid
            Guid personAliasGuid = Guid.Empty;
            string personAttribute = GetAttributeValue( action, "Person" );

            Guid guid = personAttribute.AsGuid();
            if (!guid.IsEmpty())
            {
                var attribute = AttributeCache.Read( guid, rockContext );
                if ( attribute != null )
                {
                    string value = action.GetWorklowAttributeValue(guid);
                    personAliasGuid = value.AsGuid();
                }

                if ( personAliasGuid != Guid.Empty )
                {
                    person = new PersonAliasService(rockContext).Queryable().AsNoTracking()
                                    .Where(p => p.Guid.Equals(personAliasGuid))
                                    .Select(p => p.Person)
                                    .FirstOrDefault();
                }
                else {
                    errorMessages.Add("The person could not be found in the attribute!");
                }
            }

            // get attendance date
            Guid dateTimeAttributeGuid = GetAttributeValue(action, "AttendanceDatetime").AsGuid();
            if ( !dateTimeAttributeGuid.IsEmpty() )
            {
                string attributeDatetime = action.GetWorklowAttributeValue(dateTimeAttributeGuid);

                if ( string.IsNullOrWhiteSpace(attributeDatetime) )
                {
                    attendanceDateTime = RockDateTime.Now;
                }
                else
                {
                    if ( !DateTime.TryParse(attributeDatetime, out attendanceDateTime) )
                    {
                        errorMessages.Add(string.Format("Could not parse the date provided {0}.", attributeDatetime));
                    }
                }
            }

            // get add to group
            addToGroup = GetAttributeValue(action, "AddToGroup").AsBoolean();

            // get campus
            Guid campusGuid = Guid.Empty;
            Guid campusAttributeGuid = GetAttributeValue(action, "Campus").AsGuid();
            if ( !campusAttributeGuid.IsEmpty() )
            {
                var campusAttribute = AttributeCache.Read(campusAttributeGuid, rockContext);

                if ( campusAttribute != null )
                {
                    campusGuid = action.GetWorklowAttributeValue(campusAttributeGuid).AsGuid();
                }
            }

            // set attribute
            if ( groupGuid.HasValue && person != null && attendanceDateTime != DateTime.MinValue )
            {
                var group = new GroupService(rockContext).Queryable("GroupType.DefaultGroupRole")
                                            .Where(g => g.Guid == groupGuid)
                                            .FirstOrDefault();
                if ( group != null )
                {
                    GroupMemberService groupMemberService = new GroupMemberService(rockContext);

                    // get group member
                    var groupMember = groupMemberService.Queryable()
                                            .Where(m => m.Group.Guid == groupGuid
                                                && m.PersonId == person.Id)
                                            .FirstOrDefault();
                    if ( groupMember == null )
                    {
                        if ( addToGroup )
                        {


                            if ( group != null )
                            {
                                groupMember = new GroupMember();
                                groupMember.GroupId = group.Id;
                                groupMember.PersonId = person.Id;
                                groupMember.GroupMemberStatus = GroupMemberStatus.Active;
                                groupMember.GroupRole = group.GroupType.DefaultGroupRole;
                                groupMemberService.Add(groupMember);
                                rockContext.SaveChanges();
                            }
                        }
                        else
                        {
                            action.AddLogEntry(string.Format("{0} was not a member of the group {1} and the action was not configured to add them.", person.FullName, group.Name));
                            return true;
                        }
                    }

                    AttendanceService attendanceService = new AttendanceService(rockContext);

                    Attendance attendance = new Attendance();
                    attendance.GroupId = group.Id;
                    attendance.PersonAliasId = person.PrimaryAliasId;
                    attendance.StartDateTime = attendanceDateTime;
                    attendance.DidAttend = true;

                    if ( campusGuid != Guid.Empty )
                    {
                        var campus = new CampusService(rockContext).Queryable().AsNoTracking()
                                            .Where(l => l.Guid == campusGuid)
                                            .FirstOrDefault();

                        if ( campus != null )
                        {
                            attendance.CampusId = campus.Id;
                        }
                    }

                    attendanceService.Add(attendance);
                    rockContext.SaveChanges();                    
                }
                else
                {
                    errorMessages.Add(string.Format("Could not find group matching the guid '{0}'.", groupGuid));
                }
            }

            errorMessages.ForEach( m => action.AddLogEntry( m, true ) );

            return true;
        }

    }
}