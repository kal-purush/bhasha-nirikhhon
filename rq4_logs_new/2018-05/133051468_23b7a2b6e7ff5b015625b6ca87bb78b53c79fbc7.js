module.exports = {
  uriPath: '/api/3',
  lists: {
    AFFILIATE: 'attendee',
    CES: 'company-exhibitor-sponsor'
  },
  tags: {
    AFFILIATE: 'Interest-Affiliate',
    CES: 'Interest-Exhibitor-&-Sponsor'
  },
  ticket: {
    AFFILIATE: 'affiliate',
    AFFILIATEPLUS: 'affiliate-plus',
    COMPANY: 'company',
    COMPANYPLUS: 'company-plus',
    EXHIBITOR: 'exhibitor',
    SPONSOR: 'sponsor'
  },
  relatedTags: {
    CES: [
      'Europe-Company',
      'Europe-ComPlus',
      'Europe-Exhibitor',
      'Europe-Sponsor',
      'Asia-Company',
      'Asia-ComPlus',
      'Asia-Exhbitor',
      'Asia-Sponsor'
    ],
    AFFILIATE: [
      'Europe-Affiliate',
      'Asia-Affiliate'
    ]
  },
  method: {
    SIGNUP: 'signup'
  },
  events: {
    ALL: 'All',
    EUROPE: 'Europe',
    ASIA: 'Asia'
  },
  types: {
    ALL: 'All',
    AFFILIATE: 'Affiliate',
    EXHIBITOR: 'Exhibitor'
  }
}