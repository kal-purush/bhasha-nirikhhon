package comun;

public interface Identificable<T extends Comparable<T>> {//extends Comparable<Identificable<T>> {//, V extends Identificable<T, V>> {

	T getId();
	
//	@Override
//	default int compareTo(Identificable<T> identificable) {
//		return getId().compareTo(identificable.getId());
//	}
	
//	default Identificable<T, V> getValor() {
//		return this;
//	}
	
}