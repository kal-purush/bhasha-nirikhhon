/** model for the Measure object */
export class Measure {
    id: string;
    dateMeasured: string;
    value: string;
    aggregateId: string;
    aggregateName: string;
    aggregateType: string;
    thresholdId: string;
    thresholdName: string;

    constructor(id: string, dateMeasured: string,value: string,aggregateId: string,aggregateName: string,aggregateType: string,thresholdId: string,thresholdName: string) {
        this.id = id;
        this.dateMeasured = dateMeasured;
        this.value = value;
        this.aggregateId = aggregateId;
        this.aggregateName = aggregateName;
        this.aggregateType = aggregateType;
        this.thresholdId = thresholdId;
        this.thresholdName = thresholdName;
    }
    
}