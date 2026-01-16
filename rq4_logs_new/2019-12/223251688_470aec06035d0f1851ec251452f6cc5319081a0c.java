package origamieditor3d.origami;

public abstract class CommandFold implements Command {
	
	double [] ppoint;
	double [] pnormal;
	OrigamiGen1 origami;
	
	@Override
	public double[] getPpoint() {
		return this.ppoint;
	}
	
	@Override
	public double[] getPnormal() {
		return this.pnormal;
	}
	

}