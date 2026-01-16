class Re301

{
public static void Sum(int... a)
{
int total=0;
for(int i=0;i<a.length;i++)
total=total+a[i];
System.out.println(total);
}
public static void main (String kl[])
{
Sum();
Sum(10);
Sum(10,20);
Sum(10,20,30);
}
}