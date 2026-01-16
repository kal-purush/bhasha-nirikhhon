package modeltest;

import static org.junit.Assert.*;

import org.junit.Test;

import model.util.ModelException;

public class ModelExceptionTest {

	@Test
	public void testModelException() {
		ModelException me= new ModelException();
		ModelException me1= new ModelException("mensaje de error");
		ModelException me2= new ModelException(new NullPointerException());
		ModelException me3= new ModelException("fuera de indice en la estructura del modelo que sea",new IndexOutOfBoundsException());
		
		
		assertNotNull(me);
		assertNotNull(me1);
		assertNotNull(me2);
		assertNotNull(me3);
		
		//datos me1
		assertNotNull(me1.getMessage());
		assertTrue(me1.getMessage().equals("mensaje de error"));
		//datos me2
		assertNotNull(me2.getCause());
		assertTrue(me2.getCause() instanceof NullPointerException );
		//datos me3
		assertNotNull(me3.getMessage());
		assertNotNull(me3.getCause());
		assertTrue(me3.getCause() instanceof IndexOutOfBoundsException && me3.getMessage().equals("fuera de indice en la estructura del modelo que sea") );
	}

}