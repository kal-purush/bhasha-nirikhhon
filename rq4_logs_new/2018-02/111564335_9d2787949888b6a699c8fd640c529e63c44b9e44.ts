import getMessage from 'util/i18n/i18nUtils';
import InjectedIntl = ReactIntl.InjectedIntl;

const stepIntlIds = [
    {
        fortsettKnappLabelIntlId: 'standard.button.neste',
        stegIndikatorLabelIntlId: 'relasjonBarn.sectionheading.relasjonBarn'
    },
    {
        fortsettKnappLabelIntlId: 'standard.button.neste',
        stegIndikatorLabelIntlId: 'medlemmskap.sectionheading.medlemmskap'
    },
    {
        fortsettKnappLabelIntlId: 'standard.sectionheading.sendSoknad',
        stegIndikatorLabelIntlId: 'oppsummering.sectionheading.oppsummering'
    }
];

export default (intl: InjectedIntl) => stepIntlIds.map((step) => ({
    fortsettKnappLabel: getMessage(intl, step.fortsettKnappLabelIntlId),
    stegIndikatorLabel: getMessage(intl, step.stegIndikatorLabelIntlId)
}));