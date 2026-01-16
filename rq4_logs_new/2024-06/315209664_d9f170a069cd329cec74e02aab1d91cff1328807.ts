import { defineMessages } from 'react-intl'

const membershipCardMessages = {
  MembershipCardDiscount: defineMessages({
    ActivityTicket: { id: 'membershipCard.MembershipCardDiscount.ActivityTicket', defaultMessage: 'Activity' },
    ProgramPlan: { id: 'membershipCard.MembershipCardDiscount.ProgramPlan', defaultMessage: 'Program' },
    ProgramPackagePlan: {
      id: 'membershipCard.MembershipCardDiscount.ProgramPackagePlan',
      defaultMessage: 'Program Package',
    },
    PodcastProgram: { id: 'membershipCard.MembershipCardDiscount.PodcastProgram', defaultMessage: 'Podcast' },
    cashDiscount: {
      id: 'membershipCard.MembershipCardDiscount.cashDiscount',
      defaultMessage: ' Cash discount {amount}',
    },
    percentageDiscount: {
      id: 'membershipCard.MembershipCardDiscount.percentageDiscount',
      defaultMessage: 'Percentage discount {amount}%',
    },
    generalDiscount: { id: 'membershipCard.MembershipCardDiscount.generalDiscount', defaultMessage: 'Discount' },
    type: { id: 'membershipCard.MembershipCardDiscount.type', defaultMessage: 'Type' },
    discountName: { id: 'membershipCard.MembershipCardDiscount.discountName', defaultMessage: 'Name' },
    discountType: { id: 'membershipCard.MembershipCardDiscount.discountType', defaultMessage: 'Discount Term' },
    equityType: { id: 'membershipCard.MembershipCardDiscount.equityType', defaultMessage: 'Equity' },
    usageDescription: {
      id: 'membershipCard.MembershipCardDiscount.usageDescription',
      defaultMessage: 'Usage Description',
    },
    discountTerms: { id: 'membershipCard.MembershipCardDiscount.discountTerms', defaultMessage: 'Discount Terms' },
    editDiscountTerm: { id: 'membershipCard.MembershipCardDiscount.editDiscountTerm', defaultMessage: 'Edit Term' },
    notYetSettingDiscountTerm: {
      id: 'membershipCard.MembershipCardDiscount.notYetSettingDiscountTerm',
      defaultMessage: 'Discount terms have not been set yet.',
    },
    createDiscountTerm: {
      id: 'membershipCard.MembershipCardDiscount.createDiscountTerm',
      defaultMessage: 'Create Discount Term',
    },
    deleteDiscountTerm: {
      id: 'membershipCard.MembershipCardDiscount.deleteDiscountTerm',
      defaultMessage: 'Delete Discount Term',
    },
    cancel: {
      id: 'membershipCard.MembershipCardDiscount.cancel',
      defaultMessage: 'Cancel',
    },
    confirm: {
      id: 'membershipCard.MembershipCardDiscount.confirm',
      defaultMessage: 'Confirm',
    },
    discountScope: {
      id: 'membershipCard.MembershipCardDiscount.discountScope',
      defaultMessage: 'Discount Scope',
    },
    duplicateError: {
      id: 'membershipCard.MembershipCardDiscount.duplicateError',
      defaultMessage: 'The condition has been set repeatedly, please check again.',
    },
  }),
}

export default membershipCardMessages