import {Meteor} from "meteor/meteor";
import WorkersClient from "/server/workflow";
import {PerfWorkflowTasks} from "/imports/ui/model/perf-workflow-tasks";

export const get_user_permitted_tasks = () => {
  // Get list from group too
  const currentUserGroups = Meteor.user()?.tequila?.group
  let currentUserGroupsArray: string[] = currentUserGroups?.split(',') || [];

  return WorkersClient.find({
    $or: [
      {"customHeaders.allowed_groups": {$in: currentUserGroupsArray}},
      {"variables.assigneeSciper": Meteor.user()?._id}
    ]
  })
}

export const is_allowed_to_submit = (taskKey: string) : boolean => {
  const currentUserGroups = Meteor.user()?.tequila?.group
  let currentUserGroupsArray: string[] = currentUserGroups?.split(',') || [];

  return PerfWorkflowTasks.find({$and : [
      {
        "key": taskKey
      },
      {
        $or: [
          {"customHeaders.allowed_groups": {$in: currentUserGroupsArray}},
          {"variables.assigneeSciper": Meteor.user()?._id}
        ]
      }
    ]
    }).count() > 0
}