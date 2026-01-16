// <reference path='../model/data/database/mongodb/mongoose.d.ts'/>

import Mongoose = require('mongoose');

import Facade = require('../model/Facade');

import TEMPLATE_NAMES = require('../utils/TemplateNames');

var PartnerCompanyModel = Mongoose.model('PartnerCompanies');

class NewPartnerCompanyController {
    private facade : Facade;

    public constructor() {
        this.facade = Facade.getInstance();
    }

    public newPartnerCompany(request, reply) : void {
        reply.view(TEMPLATE_NAMES.NEW_COMPANY);
    }

    public newPartnerCompanyPOST = (request, reply) => {
        var partnerCompany = PartnerCompanyModel.hydrateFromPlainObject(request.payload);

        this.facade.insertPartnerCompany(partnerCompany, (err, object) => {
            if (err) throw err;

            // TODO: mudar o template para o qual o usuário será redirecionado ao registrar a empresa
            reply.view(TEMPLATE_NAMES.NEW_COMPANY);
        });
    };
}

export = NewPartnerCompanyController;