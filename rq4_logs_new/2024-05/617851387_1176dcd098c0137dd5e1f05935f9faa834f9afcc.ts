import { useCallback } from 'react';
import FundCriterionRule from '../../../../props/models/FundCriterionRule';
import { dateParse } from '../../../../../dashboard/helpers/dates';
import { isBefore, isEqual, parseISO } from 'date-fns';
import RecordType from '../../../../../dashboard/props/models/RecordType';
import { LocalCriterion } from '../FundRequest';

export default function useShouldRequestRecord(
    recordTypesByKey: { [key: string]: RecordType },
    recordValuesByType: { [key: string]: string },
) {
    const shouldRequestByRule = useCallback(
        (rule: FundCriterionRule) => {
            const recordType = recordTypesByKey?.[rule?.record_type_key];
            const value = recordValuesByType?.[recordType?.key] || '';

            if (!recordType) {
                return true;
            }

            if (['bool', 'iban', 'email', 'select', 'string'].includes(recordType.type)) {
                const validOption =
                    recordType.type == 'select' || recordType.type == 'bool'
                        ? recordType.options.map((item) => item.value.toString()).includes(value.toString())
                        : true;

                switch (rule.operator) {
                    case '=':
                        return value.toString() === rule.value.toString() && validOption;
                    case '!=':
                        return value.toString() != rule.value.toString() && validOption;
                    default:
                        return true;
                }
            }

            if (recordType.type == 'date') {
                const dateValue = dateParse(value, 'dd-MM-yyyy');
                const dateRuleValue = parseISO(rule.value);

                switch (rule.operator) {
                    case '<':
                        return isBefore(dateValue, dateRuleValue);
                    case '<=':
                        return isBefore(dateValue, dateRuleValue) || isEqual(dateValue, dateRuleValue);
                    case '>':
                        return isBefore(dateRuleValue, dateValue);
                    case '>=':
                        return isBefore(dateRuleValue, dateValue) || isEqual(dateValue, dateRuleValue);
                    case '=':
                        return isEqual(dateValue, dateRuleValue);
                    case '!=':
                        return !isEqual(dateValue, dateRuleValue);
                    default:
                        return true;
                }
            }

            if (recordType.type == 'number' || recordType.type == 'select_number') {
                const numberValue = Number(value);
                const numberRuleValue = Number(rule.value);

                const validOption =
                    recordType.type == 'select_number'
                        ? recordType.options.map((item) => Number(item.value)).includes(numberValue)
                        : true;

                switch (rule.operator) {
                    case '<':
                        return numberValue < numberRuleValue && validOption;
                    case '<=':
                        return numberValue <= numberRuleValue && validOption;
                    case '>':
                        return numberValue > numberRuleValue && validOption;
                    case '>=':
                        return numberValue >= numberRuleValue && validOption;
                    case '=':
                        return numberValue === numberRuleValue && validOption;
                    case '!=':
                        return numberValue != numberRuleValue && validOption;
                    default:
                        return true;
                }
            }
        },
        [recordValuesByType, recordTypesByKey],
    );

    return useCallback(
        (criterion: LocalCriterion) => {
            return criterion.rules.filter((rule) => shouldRequestByRule(rule)).length === criterion.rules.length;
        },
        [shouldRequestByRule],
    );
}