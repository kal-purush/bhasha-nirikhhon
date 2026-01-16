import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Test {

	public static void main(String args[]) {
		ArrayList lista = new ArrayList<String>();
		lista.add("Luis");
		lista.add("Andres");
		lista.add("Julio");
		
		Collections.sort(lista);
		
		System.out.println(lista);
		
		List<Auto> lista2 = new ArrayList<Auto>();
		lista2.add(new Auto("Mercedes"));
		lista2.add(new Auto("Audi"));
		lista2.add(new Auto("Renault"));
		
		
		Collections.sort(lista2,Collections.reverseOrder());
		
		//lista2.forEach((a)->System.out.print(a.nombre));
		
		
		
		
		/*for(Auto a: lista2)
			System.out.println(a.nombre);
		
		byte x = 30;
		byte y = 65;
		Test2 test2 = new Test2(x,y);
		
		Zebra e = new Zebra();
		*/
		
		Animalv2 gato = ()-> {System.out.println("miauuu");};
		gato.hacerRuido();
		
	}
}

interface Animalv2{
	public void hacerRuido();
}

class Auto implements Comparable{
	
	String nombre;

	public Auto(String nombre) {
		this.nombre = nombre;
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return this.nombre.compareTo(((Auto)o).nombre);
	}
	
	
}

class Test2
{
Test2(int x, int y)
{
System.out.println("x = "+x+" y = "+y);
}
Test2(int x, float y)
{
System.out.println("x = "+x+" y = "+y);
}
}
class Animal
{
public Animal()
{
System.out.println("Animal class constructor called");
}
}
class Zebra extends Animal
{
public Zebra()
{
System.out.println("Zebra class constructor called");
}
}