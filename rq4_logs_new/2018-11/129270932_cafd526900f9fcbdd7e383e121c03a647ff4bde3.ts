import { Pairing } from "../Models";
import { AnalyserHelperTestData } from "./AnalysisHelper.spec";
import { DefaultPattern } from "./Models";
import { ThreeBlackCrows } from "./ThreeBlackCrows";

const data = new AnalyserHelperTestData();

test('ThreeBlackCrows analysis is skipped because of parameters', () => {
    const threeBlackCrows = new ThreeBlackCrows();
    let result = threeBlackCrows.analyse(null as unknown as Pairing[], 2, true);
    expect(result).toEqual(DefaultPattern);

    result = threeBlackCrows.analyse([data.two, data.two], 4, true);
    expect(result).toEqual(DefaultPattern);

    result = threeBlackCrows.analyse([data.two, data.two, data.two, data.two], 3, true);
    expect(result).toEqual(DefaultPattern);

    result = threeBlackCrows.analyse([data.two, data.two, data.two, data.two, data.two], 4, true);
    expect(result).toEqual(DefaultPattern);
});

test('ThreeBlackCrows analysis is Sell', () => {
    const threeBlackCrows = new ThreeBlackCrows();
    const result = threeBlackCrows.analyse([data.four, data.three, data.two, data.one, data.current], 4, false);
    const expected = data.sellPattern;
    expected.confidence = 78;
    expected.comment = "Three Black Crows pattern recognized.";
    expect(result).toEqual(expected);
});

test('ThreeBlackCrows analysis is Nothing', () => {
    const threeBlackCrows = new ThreeBlackCrows();
    const result = threeBlackCrows.analyse([data.one, data.three, data.two, data.current, data.one], 4, false);
    expect(result).toEqual(DefaultPattern);
});