public class Mapper<D> {
	private Mapping<D> map = null;

	public Mapper(Mapping<D> map) {
		this.map = map;
	}

	public D apply(D data) {
		System.out.println(data);
		return map.apply(data);
	}
}