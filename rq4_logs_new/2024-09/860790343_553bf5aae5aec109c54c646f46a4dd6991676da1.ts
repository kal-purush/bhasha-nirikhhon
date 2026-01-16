import * as azure from "@pulumi/azure-native";
import { getGroupName } from "@az-commons";
import * as config from "../config";

// Create Shared Resource Group
const rsGroup = new azure.resources.ResourceGroup(
  getGroupName(config.azGroups.cloudPC),
);

// Export the information that will be used in the other projects
export default {
  rsGroup: { name: rsGroup.name, id: rsGroup.id },
};