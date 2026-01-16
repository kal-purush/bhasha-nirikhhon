export interface IConditionalDisplay {
  field: string,
  condition: ICondition
};

export interface ICondition {
  field: string,
  value: any,
  operator: string,
  or?:  ICondition[],
  and?: ICondition[]
};

function extractFieldValue(object, condition: ICondition){
  let propsChain = condition.field.split('.');
  let value = object;
  _.each(propsChain, function(propName){ value = value[propName]; });
  return value;
}

function evaluateCondition(object: any, condition: ICondition): Boolean {
  let value = extractFieldValue(object, condition);
  switch (condition.operator) {
    case ">":  return value > condition.value;
    case "<":  return value < condition.value;
    case "==": return value == condition.value;
    case ">=": return value >= condition.value;
    case "<=": return value <= condition.value;
  }
}

export class ConditionalDisplay {

  private visibility = {};

  constructor(private object, private conditions: IConditionalDisplay[]){
    this.updateVisibility();
  }

  updateVisibility(): void{
    _.each(this.conditions, (condition)=>{
      this.visibility[condition.field] = evaluateCondition(this.object, condition.condition);
    });
  }

  isVisible(fieldName): Boolean{
    if (_.has(this.visibility, fieldName))
      return !!this.visibility[fieldName];
    else
      return true;
  }

}