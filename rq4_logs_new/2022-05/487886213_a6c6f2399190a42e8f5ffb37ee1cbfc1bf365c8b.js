const CompaniesCollection = Backbone.Collection.extend({
  url: '../../db/companies.json',
  model: CompanyModel
})