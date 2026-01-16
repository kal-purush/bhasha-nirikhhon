package classes;

import java.util.Iterator;
import java.util.List;

import interfaces.Mapping;
import interfaces.Operator;
import interfaces.Predicate;
import interfaces.Stream;

public class ArrayStream<E> implements Stream<E>{

	@Override
	public Iterator<E> iterator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean matchAll(Predicate<? super E> predicate) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean matchAny(Predicate<? super E> predicate) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int countAll() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int count(Predicate<? super E> predicate) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public E get(int index) throws IndexOutOfBoundsException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public E find(Predicate<? super E> predicate) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public E reduce(Operator<E> operator) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<E> toList() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Stream<E> limit(int n) throws IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Stream<E> skip(int n) throws IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Stream<E> filter(Predicate<? super E> predicate) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <F> Stream<F> map(Mapping<? super E, ? extends F> mapping) {
		// TODO Auto-generated method stub
		return null;
	}

}