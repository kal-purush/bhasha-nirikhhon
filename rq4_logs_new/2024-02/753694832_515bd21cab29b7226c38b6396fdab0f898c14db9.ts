export default class PositionMeta {
    public readonly uniquePositions = new Set<string>();
    public readonly globalMin;
    public readonly globalMax;

    public constructor(uniquePositions: Set<string>, globalMin: number[], globalMax: number[]) {
        this.uniquePositions = uniquePositions;
        this.globalMin = globalMin;
        this.globalMax = globalMax;
    }
}