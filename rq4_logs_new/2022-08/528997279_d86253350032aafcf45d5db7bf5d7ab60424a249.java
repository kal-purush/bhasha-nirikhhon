package Item;

public class Application {

	public static void main(String[] args) {
Item<String> item1 = new Item<String>();
item1.setE("brad");

Item<Integer> item2 = new Item<Integer>();
item2.setE(10);

SmallBag<Item> smallBag1 = new SmallBag<Item>();
smallBag1.setItem(item1);
System.out.println(smallBag1.getItem().getE());

smallBag1.setItem(item2);
System.out.println(smallBag1.getItem().getE());


	}

}