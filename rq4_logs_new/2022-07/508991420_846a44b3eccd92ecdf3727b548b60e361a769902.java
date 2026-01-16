/*
 * The contents of this file are subject to the OpenMRS Public License
 * Version 1.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://license.openmrs.org
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * Copyright (C) OpenMRS, LLC.  All Rights Reserved.
 */


package org.openmrs.module.hospitalcore;

/**
 *
 */
public class BillingConstants {

	public static final String MODULE = "billing";

	public static final String PRIV_VIEW_BILL = "View Bills";

	public static final String PRIV_ADD_BILL = "Add Bill";

	public static final String PRIV_EDIT_BILL = "Edit Bill";

	public static final String PRIV_EDIT_BILL_ONCE_PRINTED = "Edit Bill once printed";

	public static final String PRIV_VIEW_TENDERS = "View Tenders";

	public static final String PRIV_ADD_EDIT_TENDER = "Add/Edit Tender";

	public static final String PRIV_DELETE_TENDER = "Delete Tender";

	public static final String PRIV_VIEW_COMPANIES = "View Companies";

	public static final String PRIV_ADD_EDIT_COMPANY = "Add/Edit Company";

	public static final String PRIV_DELETE_COMPANY = "Delete Company";

	public static final String PRIV_VIEW_DRIVERS = "View Drivers";

	public static final String PRIV_ADD_EDIT_DRIVER = "Add/Edit Driver";

	public static final String PRIV_DELETE_DRIVER = "Delete Driver";

	public static final String PRIV_VIEW_AMBULANCES = "View Ambulances";

	public static final String PRIV_ADD_EDIT_AMBULANCE = "Add/Edit Ambulance";

	public static final String PRIV_DELETE_AMBULANCE = "Delete Ambulance";

	public static final String TRUE = "t";

	public static final String FALSE = "f";

	public static final String GLOBAL_PROPRETY_SERVICE_CONCEPT = "billing.rootServiceConceptId";

	public static final String PRIV_DELETE_MISCELLANEOUS_SERVICE = "Delete Miscellaneous Service";

	public static final String PRIV_ADD_EDIT_MISCELLANEOUS_SERVICE = "Add/Edit Miscellaneous Service";

	public static final String PRIV_VIEW_MISCELLANEOUS_SERVICE = "View Miscellaneous Service";

	public static final String GLOBAL_PROPRETY_LAB_ENCOUNTER_TYPE = MODULE
			+ ".labEncounterType";

	public static final String GLOBAL_PROPRETY_RADIOLOGY_ENCOUNTER_TYPE = MODULE
			+ ".radiologyEncounterType";

	public static final String GLOBAL_PROPRETY_LAB_ORDER_TYPE = MODULE
			+ ".labOrderType";

	public static final String GLOBAL_PROPRETY_RADIOLOGY_ORDER_TYPE = MODULE
			+ ".radiologyOrderType";
	
	//ghanshyam 3-june-2013 New Requirement #1632 Orders from dashboard must be appear in billing queue.User must be able to generate bills from this queue
	public static final Integer PAGESIZE = 20;

}