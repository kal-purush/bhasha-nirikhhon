package com.soffid.iam.addons.federation.model;

import com.soffid.iam.addons.federation.common.FederationMemberSession;

public class FederationMemberSessionEntityDaoImpl extends FederationMemberSessionEntityDaoBase {

	@Override
	public void toFederationMemberSession(FederationMemberSessionEntity source, FederationMemberSession target) {
		super.toFederationMemberSession(source, target);
		target.setFederationMember(source.getFederationMember().getPublicKey());
	}

	@Override
	public void federationMemberSessionToEntity(FederationMemberSession source, FederationMemberSessionEntity target,
			boolean copyIfNull) {
		super.federationMemberSessionToEntity(source, target, copyIfNull);
		target.setFederationMember(null);
		if (source.getFederationMember() != null && !source.getFederationMember().trim().isEmpty())
			for (FederationMemberEntity fm: getFederationMemberEntityDao().findFMByPublicId(source.getFederationMember()))
				target.setFederationMember(fm);
	}
}