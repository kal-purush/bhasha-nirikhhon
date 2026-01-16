declare var Percenter: any;

class Calc {
	public static Value1: number = 1;
	public static Value2: number = 1;
	public static ResultAugmentation: number = 1;
	public static getPercentage(): number {
		return ((this.Value2 * 100 / this.Value1) - 100) * this.ResultAugmentation;
	}
}

Percenter.Calc = Calc;