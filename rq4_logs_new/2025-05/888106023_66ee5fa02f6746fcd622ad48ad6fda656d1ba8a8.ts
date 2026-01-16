import { IRuleDecision, ValueDataType } from "@isettingkit/input";
import { dataTranslations } from "@utils/dataTranslations";

const decisionScoreModelsConfig = (
  {
    ruleName,
    labelName,
    howToSetTheDecision,
    decisionDataType,
    conditionsThatEstablishesTheDecision,
    listOfPossibleValues,
  }: IRuleDecision,
  nameRule?: string,
  businessUnit?: string,
) => {
  if (labelName && decisionDataType) {
    const decisionData = decisionDataType.toLocaleUpperCase();

    const decisionTemplate: IRuleDecision = {
      ruleName: ruleName,
      labelName: "Modelo de score",
      decisionDataType:
        ValueDataType[decisionData as keyof typeof ValueDataType],
      howToSetTheDecision: howToSetTheDecision,
      value: nameRule,
      effectiveFrom: "",
      validUntil: "",
      listOfPossibleValues: listOfPossibleValues,
    };

    if (
      conditionsThatEstablishesTheDecision &&
      conditionsThatEstablishesTheDecision?.length > 0
    ) {
      decisionTemplate.conditionsThatEstablishesTheDecision =
        conditionsThatEstablishesTheDecision.map((condition) => ({
          conditionName:
            dataTranslations[condition.conditionName] ??
            condition.conditionName,
          labelName:
            dataTranslations[condition.labelName] ?? condition.labelName,
          conditionDataType: condition.conditionDataType,
          value:
            condition.conditionName === "BusinessUnit"
              ? businessUnit
              : condition.value,
          listOfPossibleValues: condition.listOfPossibleValues,
          howToSetTheCondition: condition.howToSetTheCondition,
          hidden: condition.conditionName === "BusinessUnit" ? true : false,
        }));
    }

    return decisionTemplate;
  }
};

export { decisionScoreModelsConfig };