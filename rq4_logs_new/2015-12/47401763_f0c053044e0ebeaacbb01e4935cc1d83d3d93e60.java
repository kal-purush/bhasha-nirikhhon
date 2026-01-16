package classes;

import java.util.List;

import interfaces.Mapping;
import interfaces.Operator;
import interfaces.Predicate;
import interfaces.Stream;

public abstract class LazyStream<E> implements Stream<E> {
	
	@Override
	public boolean matchAll(Predicate<? super E> predicate) {
		//TODO
		return false;
	}

	@Override
	public boolean matchAny(Predicate<? super E> predicate) {
		//TODO
		return false;
	}

	@Override
	public int countAll(){
		//TODO
		return 0;
	}

	@Override
	public int count(Predicate<? super E> predicate) {
		//TODO
		return 0;
	}

	@Override
	public E get(int index) throws IndexOutOfBoundsException {
		//TODO
		return null; 
	}

	@Override
	public E find(Predicate<? super E> predicate) {
		//TODO
		return null;
	}

	@Override
	public E reduce(Operator<E> operator) {
		//TODO
		return null;
	}

	@Override
	public List<E> toList() {
		//TODO
		return null;
	}

	@Override
	public Stream<E> limit(int n) throws IllegalArgumentException {
		//TODO
		return null;
	}

	@Override
	public Stream<E> skip(int n) throws IllegalArgumentException {
		//TODO
		return null;
	}

	@Override
	public Stream<E> filter(Predicate<? super E> predicate) {
		//TODO
		return null;
	}

	public <F> Stream<F> map(Mapping<? super E, ? extends F> mapping) {
		//TODO
		return null;
	}
	
	
}