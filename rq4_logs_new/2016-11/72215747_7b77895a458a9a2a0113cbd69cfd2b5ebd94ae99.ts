import { JsonProperty, JsonArray } from '../../src/';

export class NumberTests {
    @JsonProperty('nullValue')
    public nullValue: number;

    @JsonProperty('undefinedValue')
    public undefinedValue: number;

    @JsonProperty('falseValue')
    public falseValue: number;

    @JsonProperty('trueValue')
    public trueValue: number;

    @JsonProperty('emptyString')
    public emptyString: number;

    @JsonProperty('nonemptyString')
    public nonemptyString: number;

    @JsonProperty('intString')
    public intString: number;

    @JsonProperty('floatString')
    public floatString: number;

    @JsonProperty('emptyArray')
    public emptyArray: number;

    @JsonProperty('nonemptyArray')
    public nonemptyArray: number;

    @JsonProperty('object')
    public object: number;

    @JsonProperty('negativeOne')
    public negativeOne: number;

    @JsonProperty('zero')
    public zero: number;

    @JsonProperty('one')
    public one: number;

}