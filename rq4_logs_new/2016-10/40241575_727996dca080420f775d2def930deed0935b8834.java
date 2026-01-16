package dailyPk;

import java.util.ArrayList;

class s1 implements  Comparable<s1>
{
	private String _name=null;
	private s1(){}
	public s1(final String name)
	{
		_name =name;
	}
	public String getname()
	{
		return _name;
	}
	@Override
	public int compareTo(final s1 o) {
		// TODO Auto-generated method stub
		final Integer p = _name.compareTo(o.getname());
		return p;
	}


}

public class Sort1_10_12 {

	private static<T> void exchange(final Integer m,final Integer n,final ArrayList<Comparable<T>>A)
	{
		final Comparable<T> tmp =A.get(m);
		A.set(m,A.get(n));
		A.set(n, tmp);
	}
	private static<T> Integer getPivot(final Integer lo, final Integer hi, final ArrayList<Comparable<T> > A)
	{
		final Integer p ;
		Integer i= new Integer(lo);
		Integer j = new Integer(hi)+1;
		final T seed = (T) A.get(lo);

		while(true)
		{
			while(A.get(++i).compareTo(seed)>0) {
				if (i==j) {
					break;
				}
			}
			while(A.get(--j).compareTo(seed)<0) {
				if(j==i) {
					break;
				}
			}
			if(i>=j) {
				break;
			}
			exchange(i,j,A);
		}
		exchange(lo,j,A);
		return j;
	}
	private static  <T> void Sort1(final Integer lo, final Integer hi, final ArrayList<Comparable<T> > A)
	{
		if(lo >= hi) {
			return;
		}

		final Integer p = getPivot(lo,hi,A);

		Sort1(lo,p-1,A);
		Sort1(p+1,hi,A);

	}
	public static void main(final String[] args) {
		// TODO Auto-generated method stub
		final s1[] A={new s1("xiao"),new s1("bi"),new s1("wo"), new s1("chi")};

	}

}