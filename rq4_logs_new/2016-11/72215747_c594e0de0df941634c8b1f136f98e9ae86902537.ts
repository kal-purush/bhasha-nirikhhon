import { JsonProperty, JsonArray } from '../../src/';

export class StringTests {
    @JsonProperty('nullValue')
    public nullValue: string;

    @JsonProperty('undefinedValue')
    public undefinedValue: string;

    @JsonProperty('falseValue')
    public falseValue: string;

    @JsonProperty('trueValue')
    public trueValue: string;

    @JsonProperty('emptyString')
    public emptyString: string;

    @JsonProperty('nonemptyString')
    public nonemptyString: string;

    @JsonProperty('emptyArray')
    public emptyArray: string;

    @JsonProperty('nonemptyArray')
    public nonemptyArray: string;

    @JsonProperty('object')
    public object: string;

    @JsonProperty('negativeOne')
    public negativeOne: string;

    @JsonProperty('zero')
    public zero: string;

    @JsonProperty('one')
    public one: string;

}