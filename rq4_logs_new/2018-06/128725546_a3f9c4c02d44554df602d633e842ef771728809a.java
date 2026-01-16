package application;

import java.util.List;

public interface IAlgorithm {
	public List<Point> nextList();
	public double nextValue();
	public int getAlgType(); // return 1 when u wand use nextValue method, return 2 to use nextList method;
}