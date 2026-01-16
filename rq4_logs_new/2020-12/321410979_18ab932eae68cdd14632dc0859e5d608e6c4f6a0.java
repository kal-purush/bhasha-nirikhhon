/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.openmrs.module.openhmis.inventory.api;

import java.util.List;
import org.openmrs.annotation.Authorized;
import org.openmrs.module.openhmis.commons.api.PagingInfo;
import org.openmrs.module.openhmis.commons.api.entity.IMetadataDataService;
import org.openmrs.module.openhmis.inventory.api.model.ConsumptionSummary;
import org.openmrs.module.openhmis.inventory.api.model.PharmacyConsumptionSummary;
import org.openmrs.module.openhmis.inventory.api.util.PrivilegeConstants;
import org.springframework.transaction.annotation.Transactional;

/**
 * @author MORRISON.I
 */
public interface IPharmacyConsumptionSummaryDataService extends IMetadataDataService<PharmacyConsumptionSummary> {

	@Transactional(readOnly = true)
	@Authorized({ PrivilegeConstants.MANAGE_OPERATIONS })
	List<PharmacyConsumptionSummary> getConsumptionSummarys(PharmacyConsumptionSummary consumptionSummary,
	        PagingInfo pagingInfo);

}