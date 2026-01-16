import { Record } from '../../types';
import { RecordStatus } from '../../types/enums';

export const mapRecordToValues = (
  {
    id,
    status = RecordStatus.DRAFT,
    dataProcessorContactDetails: { name = '', email = '', phone = '' } = {},
    dataProcessingAgreements = [{ dataProcessorName: '', agreementUrl: '' }],
    commonDataControllerContact: {
      companies = '',
      distributionOfResponsibilities = '',
      contactPoints = [{ name: '', email: '', phone: '' }]
    } = {},
    title = '',
    purpose = '',
    dataSubjectCategories = [],
    articleSixBasis = [{ legality: '', referenceUrl: '' }],
    otherArticles: {
      articleNine: {
        checked: articleNineChecked = false,
        referenceUrl: articleNineReferenceUrl = ''
      } = {},
      articleTen: {
        checked: articleTenChecked = false,
        referenceUrl: articleTenReferenceUrl = ''
      } = {}
    } = {},
    businessAreas = [],
    relatedDatasets = [],
    personalDataCategories = [],
    securityMeasures = '',
    plannedDeletion = '',
    highPrivacyRisk = undefined,
    dataProtectionImpactAssessment: {
      conducted = undefined,
      assessmentReportUrl = ''
    } = {},
    personalDataSubjects = '',
    privacyProcessingSystems = '',
    recipientCategories = [],
    dataTransfers: {
      transferred = undefined,
      thirdCountryRecipients = '',
      guarantees = ''
    } = {}
  }: Partial<Record>,
  organizationId: string
): Omit<Record, 'updatedAt'> => ({
  id,
  status,
  dataProcessorContactDetails: {
    name,
    email,
    phone
  },
  organizationId,
  dataProcessingAgreements,
  commonDataControllerContact: {
    companies,
    distributionOfResponsibilities,
    contactPoints
  },
  title,
  purpose,
  dataSubjectCategories,
  articleSixBasis,
  otherArticles: {
    articleNine: {
      checked: articleNineChecked,
      referenceUrl: articleNineReferenceUrl
    },
    articleTen: {
      checked: articleTenChecked,
      referenceUrl: articleTenReferenceUrl
    }
  },
  businessAreas,
  relatedDatasets,
  personalDataCategories,
  securityMeasures,
  plannedDeletion,
  highPrivacyRisk,
  dataProtectionImpactAssessment: {
    conducted,
    assessmentReportUrl
  },
  personalDataSubjects,
  privacyProcessingSystems,
  recipientCategories,
  dataTransfers: {
    transferred,
    thirdCountryRecipients,
    guarantees
  }
});