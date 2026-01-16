package conversions;

public abstract class Numerical
{
	protected abstract String getDecimal();

	protected abstract String getBinary();

	protected abstract String getHex();

	protected abstract String getOctal();

	protected abstract String showMeToDecimal();

	protected abstract String showMeToBinary();

	protected abstract String showMeToHexadecimal();

	protected abstract String showMeToOctal();

	public abstract boolean isValid();

	public Result convert()
	{
		return new Result(getDecimal(), getBinary(), getHex(), getOctal(), showMeToDecimal(),
				showMeToBinary(), showMeToHexadecimal(), showMeToOctal());
	}
}