"use strict";

import { Queryable, QueryableInstance, QueryableCollection } from "./Queryable";
import {SiteUser, SiteUsers} from "./siteusers";
import { Util } from "../../utils/util";

/**
 * Properties that provide a getter, but no setter.
 *
 */
export interface GroupReadOnlyProperties {
    canCurrentUserEditMembership?: boolean;
    canCurrentUserManageGroup?: boolean;
    canCurrentUserViewMembership?: boolean;
    id?: number;
    isHiddenInUI?: boolean;
    loginName?: string;
    ownerTitle?: string;
    principalType?: PrincipalType;
    users?: SiteUsers;
}

/**
 * Properties that provide both a getter, and a setter.
 *
 */
export interface GroupWriteableProperties {
    allowMembersEditMembership?: boolean;
    allowRequestToJoinLeave?: boolean;
    autoAcceptRequestToJoinLeave?: boolean;
    description?: string;
    onlyAllowMembersViewMembership?: boolean;
    owner?: number | SiteUser | SiteGroup;
    requestToJoinLeaveEmailSetting?: string;
    title?: string;
}

/**
 * Group Properties
 *
 */
export interface GroupProperties extends GroupReadOnlyProperties, GroupWriteableProperties {
    __metadata: { id?: string, url?: string, type?: string };
}

/**
 * Principal Type enum
 *
 */
export enum PrincipalType {
    None = 0,
    User = 1,
    DistributionList = 2,
    SecurityGroup = 4,
    SharePointGroup = 8,
    All = 15
}

/**
 * Result from adding a group.
 *
 */
export interface GroupUpdateResult {
    group: SiteGroup;
    data: any;
}

/**
 * Results from updating a group
 *
 */
export interface GroupAddResult {
    group: SiteGroup;
    data: any;
}


/**
 * Describes a collection of site users
 *
 */
export class SiteGroups extends QueryableCollection {

    /**
     * Creates a new instance of the SiteUsers class
     *
     * @param baseUrl The url or Queryable which forms the parent of this user collection
     */
    constructor(baseUrl: string | Queryable, path = "sitegroups") {
        super(baseUrl, path);
    }

    /**
     * Adds a new group to the site collection
     *
     * @param props The properties to be updated
     */
    /*tslint:disable max-line-length */
    public add(properties: GroupWriteableProperties): Promise<GroupAddResult> {
        let postBody = JSON.stringify(Util.extend(
            {"__metadata": { "type": "SP.Group" }}, properties));
        
        return this.post({ body: postBody }).then((data) => {
            return {
                data: data,
                group: this.getById(data.Id),
            };
        });
    }

    /*tslint:enable */

    /**
     * Gets a user from the collection by email
     *
     * @param email The email of the user
     * @param expandUsersGroups boolean Whether or not to expand the user's groups.  Default: false
     */
    public getByName(groupName: string): SiteGroup {
        return new SiteGroup(this.toUrl(), `getByName('${groupName}')`);
    }

    /**
     * Gets a user from the collection by id
     *
     * @param id The id of the user
     */
    public getById(id: number) {
        return new SiteGroup(this.toUrl(), `getById('${id}')`);
    }

    /**
     * Gets a user from the collection by login name
     *
     * @param loginName The login name of the user
     */
    public getByLoginName(loginName: string): SiteGroup {
        return new SiteGroup(this.toUrl(), `getByloginName('${encodeURIComponent(loginName)}')`);
    }

    /**
     * Removes a user from the collection by id
     *
     * @param id The id of the user
     */
    public removeById(id: number | Queryable): Promise<void> {
        let postBody = "{}";
        this.append(`removeById('${id}')`);
        return this.post({postBody: postBody});
    }

    /**
     * Removes a user from the collection by login name
     *
     * @param loginName The login name of the user
     */
    public removeByLoginName(loginName: string): Promise<any> {
        let postBody = "{}";
        this.append(`removeByLoginName('${loginName}')`);
        return this.post({postBody: postBody});
    }
}


/**
 * Describes a single user
 *
 */
export class SiteGroup extends QueryableInstance {
    /**
     * Creates a new instance of the User class
     *
     * @param baseUrl The url or Queryable which forms the parent of this fields collection
     * @param path Optional, passes the path to the user
     */
    constructor(baseUrl: string | Queryable, path?: string) {
        super(baseUrl, path);
    }

    /**
     * Get's the groups for this user.
     *
     */
    public get users() { return new SiteUsers(this.toUrl(), "users"); }

    /**
    * Updates this user instance with the supplied properties
    *
    * @param properties A plain object of property names and values to update for the user
    * @param eTag Value used in the IF-Match header, by default "*"
    */
    /* tslint:disable member-access */
    public update(properties: GroupWriteableProperties, eTag = "*"): Promise<GroupUpdateResult> {

        let postBody = Util.extend({"__metadata": { "type": "SP.Group" }}, properties);

        return this.post({
            body: JSON.stringify(postBody),
            headers: {
                "IF-Match": eTag,
                "X-HTTP-Method": "MERGE",
            }
        })
            .then((data) => {

                let retGroup: SiteGroup = this;

                if (properties.hasOwnProperty("Title")) {
                    retGroup = this.getParent(SiteGroup);
                    retGroup.append(`getByTitle('${properties["Title"]}') `);
                }

                return {
                    data: data,
                    group: retGroup,
                };
            });
    }
    /* tslint:enable */

    /**
     * Delete this web
     *
     */
    public delete(): Promise<void> {
        return this.post({
            headers: {
                "X-HTTP-Method": "DELETE",
            },
        });
    }
}

export interface SiteGroupAddResult {
    group: SiteGroup;
    data: GroupProperties;
}