package pl.put.poznan.transformer.logic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import static org.mockito.Mockito.*;

import pl.put.poznan.transformer.logic.Decorators.CapitalizeTransformer;
import pl.put.poznan.transformer.logic.Decorators.InverseTransformer;
import pl.put.poznan.transformer.logic.Decorators.LowerTransformer;
import pl.put.poznan.transformer.logic.Decorators.UpperTransformer;

public class TextTransformerTest {
  @Test
  public void testDecoratorOrderCall() {
    LowerTransformer lowerTransformerMocked = mock(LowerTransformer.class);

    when(lowerTransformerMocked.transform("ORGINAL")).thenReturn("orginal");

    assertEquals("Orginal", new CapitalizeTransformer(lowerTransformerMocked).transform("ORGINAL"));
  }

  @Test
  public void testInverseLowered() {
    LowerTransformer lowerTransformerMocked = mock(LowerTransformer.class);
    when(lowerTransformerMocked.transform("ORGINAL")).thenReturn("orginal");

    assertEquals("lanigro", new InverseTransformer(lowerTransformerMocked).transform("ORGINAL"));
  }

  @Test
  public void testInverseUppered() {
    LowerTransformer lowerTransformerMocked = mock(LowerTransformer.class);
    when(lowerTransformerMocked.transform("orginal")).thenReturn("ORGINAL");

    assertEquals("LANIGRO", new InverseTransformer(lowerTransformerMocked).transform("orginal"));
  }

  @Test
  public void testDecoratorOrderCall2() {
    LowerTransformer lowerTransformerMocked = mock(LowerTransformer.class);

    when(lowerTransformerMocked.transform("ORGINAL")).thenReturn("orginal");

    UpperTransformer upperTransformer = new UpperTransformer(lowerTransformerMocked);
    assertEquals("LANIGRO", new InverseTransformer(upperTransformer).transform("ORGINAL"));
  }

  @Test
  public void testgetTransformerByNameReturnsNullOnDefault() {
    BaseTransformer baseTransformerMocked = mock(BaseTransformer.class);

    assertNull(TextTransformer.getTransformerByName("thereisnosuchdecorator", baseTransformerMocked));
  }
}