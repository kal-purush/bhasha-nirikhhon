package chapter1;

import java.util.Scanner;
import java.util.logging.Level;

public class E1_5_4 {
	static int count;
	private int[] id;
	private int[] sz;
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Scanner scanner = new Scanner(System.in);
		E1_5_4 exercise = new E1_5_4();
		exercise.initial(10);//��ʼ��id����
		while(scanner.hasNextLine())//����һ������
		{
			int a = scanner.nextInt();
			int b = scanner.nextInt();
			count = exercise.union(a, b);
			for(int i = 0; i < 10; i++)
			{
				System.out.print(exercise.id[i]);
				if(i != 9) System.out.print(" ");
			}
			System.out.println();
			for(int i = 0; i < 10; i++)
			{
				System.out.print(exercise.sz[i]);
				if(i != 9) System.out.print(" ");
			}
			System.out.println();
			System.out.println(count);

		}
	}
	
	public void initial(int N)
	{
		id = new int[N];
		for(int i = 0; i < N; i++)
		{
			id[i] = i;
		}
		sz = new int[N];
		for(int i = 0; i < N; i++)
		{
			sz[i] = 1;
		}
	}
	public int findRoot(int x)
	{
		count++;
		while(x != id[x])
		{
			x = id[x];
			count++;
		}
		return x;
	}
	public int union(int p, int q)
	{
		count = 0;
		int pRoot = findRoot(p);
		int qRoot = findRoot(q);
		if(pRoot == qRoot) return count;
		if(sz[pRoot] < sz[qRoot])
		{
			id[pRoot] = qRoot;
			sz[qRoot] += sz[pRoot];
			count++;
		}
		else {
			id[qRoot] = pRoot;
			sz[pRoot] += sz[qRoot];
			count++;
		}
		return count;
	}

}