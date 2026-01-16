public class LoaderDemo{
    public static void main(String[] args) throws Exception{
	System.out.println(LoaderDemo.class.getClassLoader());
	System.out.println(LoaderDemo.class.getClassLoader().getParent());
	System.out.println(LoaderDemo.class.getClassLoader().getParent().getParent());
	
    }
}