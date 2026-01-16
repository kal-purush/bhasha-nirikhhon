import { IOrganization } from "../models/IOrganization";
import { IGroup } from "../models/IGroup";
import { IContact } from "../models/IContact";

export class CsvOrganizationFormatter
{
    /**
     * Get the group name, and first contact name and email
     */
    public static OutputGroups(organization: IOrganization) : string
    {
        let values = obj => Object.keys(obj).map(key => obj[key]);

        let csv = "Group Name,Contact Name,Email\n";
        csv += values(organization.groups).map((group) =>
        {
            return group.name.replace(",","") + "," + group.contacts[0].name.replace(",","") + "," + group.contacts[0].email.replace(",","");
        }).join("\n");
        return csv;
    }
}