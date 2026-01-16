import IPartnerCompanyRepository = require("../repository/IPartnerCompanyRepository");
import PartnerCompany = require("../entities/PartnerCompany");

class PartnerCompanyRegister {

	repository: IPartnerCompanyRepository;

	public constructor(repository: IPartnerCompanyRepository) {
		this.repository = repository;
	}

	public insertPartnerCompany(partnerCompany: PartnerCompany) {
		return this.repository.insertPartnerCompany(partnerCompany);
	}

	public removePartnerCompany(partnerCompany: PartnerCompany) {
		return this.repository.removePartnerCompany(partnerCompany);
	}

	public updatePartnerCompany(partnerCompany: PartnerCompany) {
		return this.repository.updatesPartnerCompany(partnerCompany);
	}

	public getPartnerCompany(partnerCompany: PartnerCompany) {
		return this.repository.getPartnerCompany(partnerCompany);
	}

}

export = PartnerCompanyRegister;