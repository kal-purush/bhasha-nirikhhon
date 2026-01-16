/// <reference path="../../node_modules/@types/bluebird/index.d.ts" />

import * as Promise from "bluebird";

import { IOrganization } from "../models/IOrganization";

/**
 * Formats a crawler's organization output.
 */
export interface IFormatter {
    /**
     * Saves a crawler's organization output.
     * 
     * @param string   The output file name.
     * @param organization   A crawler's organization output.
     * @returns A Promise for saving the output.
     */
    saveOrganization(fileName: string, organization: IOrganization): Promise<void>;
}