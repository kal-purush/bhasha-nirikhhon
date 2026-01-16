/*
 * Copyright(c) 2016 - Heliosphere Corp.
 * ---------------------------------------------------------------------------
 * This file is part of the Heliosphere's project which is licensed under the
 * Apache license version 2 and use is subject to license terms. You should have
 * received a copy of the license with the project's artifact binaries and/or
 * sources.
 *
 * License can be consulted at http://www.apache.org/licenses/LICENSE-2.0
 * ---------------------------------------------------------------------------
 */
package com.heliosphere.demeter.base.resource.bundle.test;

import static org.junit.Assert.fail;

import java.text.Collator;
import java.util.Locale;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.heliosphere.demeter.base.resource.ResourceException;
import com.heliosphere.demeter.base.resource.bundle.BundleDemeterBase;
import com.heliosphere.demeter.base.resource.bundle.ResourceBundleManager;

/**
 * A test case for the {@link BundleDemeterBase} class using the {@link ResourceBundleManager} class.
 * <hr>
 * @author Resse Christophe, Heliosphere Corp. 2010-2012
 * @version 1.0.0
 */
@SuppressWarnings("nls")
public final class ResourceBundle
{
	/**
	 * Dummy message in english.
	 */
	private static final String DUMMY_ENGLISH = "A test message from component: demeter-base";

	/**
	 * Dummy message in french.
	 */
	private static final String DUMMY_FRENCH = "Un message de test du composant: demeter-base";

	/**
	 * Dummy message in german.
	 */
	private static final String DUMMY_GERMAN = "Ein Test Nachricht von der Komponente: demeter-base";

	/**
	 * Dummy message in spanish.
	 */
	private static final String DUMMY_SPANISH = "Un mensaje de prueba a partir del componente: demeter-base";

	/**
	 * Dummy message in italian.
	 */
	private static final String DUMMY_ITALIAN = "Un messaggio di prova dal componente: demeter-base";

	/**
	 * Locale for english.
	 */
	private static Locale english;

	/**
	 * Locale for french.
	 */
	private static Locale french;

	/**
	 * Locale for german.
	 */
	private static Locale german;

	/**
	 * Locale for italian.
	 */
	private static Locale italian;

	/**
	 * Locale for spanish.
	 */
	private static Locale spanish;

	/**
	 * Initialization of the test cases.
	 * <p>
	 * @throws Exception In case an error occurs during the initialization.
	 */
	@BeforeClass
	public static final void setUpBeforeClass() throws Exception
	{
		italian = new Locale("it", "IT");
		french = new Locale("fr", "FR");
		english = new Locale("en", "US");
		german = new Locale("de", "DE");
		spanish = new Locale("es", "ES");
	}

	/**
	 * Finalization of the test cases.
	 * <p>
	 * @throws Exception In case an error occurs during the finalization.
	 */
	@AfterClass
	public static final void tearDownAfterClass() throws Exception
	{
		// Empty
	}

	/**
	 * Sets up the fixture.
	 * <p>
	 * @throws Exception In case an error occurs during the setup phase.
	 */
	@Before
	public final void setUp() throws Exception
	{
		// Empty
	}

	/**
	 * Tears down the fixture.
	 * <p>
	 * @throws Exception In case an error occurs during the tear down phase.
	 */
	@After
	public final void tearDown() throws Exception
	{
		// Empty
	}

	/**
	 * Test the registration of a resource bundle in english using an
	 * enumeration class.
	 */
	@SuppressWarnings("static-method")
	@Test
	public final void registerBundleByEnumClassInEnglish()
	{
		ResourceBundleManager.setLocale(english);
		ResourceBundleManager.register(BundleDemeterBase.class);

		Assert.assertTrue(ResourceBundleManager.getLocale().getLanguage() == english.getLanguage());
	}

	/**
	 * Test the registration of a resource bundle in french using an enumeration
	 * class.
	 */
	@SuppressWarnings("static-method")
	@Test
	public final void registerBundleByEnumClassInFrench()
	{
		ResourceBundleManager.setLocale(french);
		ResourceBundleManager.register(BundleDemeterBase.class);

		Assert.assertTrue(ResourceBundleManager.getLocale().getLanguage() == french.getLanguage());
	}

	/**
	 * Test the registration of a resource bundle in german using an enumeration
	 * class.
	 */
	@SuppressWarnings("static-method")
	@Test
	public final void registerBundleByEnumClassInGerman()
	{
		ResourceBundleManager.setLocale(german);
		ResourceBundleManager.register(BundleDemeterBase.class);

		Assert.assertTrue(ResourceBundleManager.getLocale().getLanguage() == german.getLanguage());
	}

	/**
	 * Test the registration of a resource bundle in spanish using an
	 * enumeration class.
	 */
	@SuppressWarnings("static-method")
	@Test
	public final void registerBundleByEnumClassInSpanish()
	{
		ResourceBundleManager.setLocale(spanish);
		ResourceBundleManager.register(BundleDemeterBase.class);

		Assert.assertTrue(ResourceBundleManager.getLocale().getLanguage() == spanish.getLanguage());
	}

	/**
	 * Test the registration of a resource bundle in italian using an
	 * enumeration class.
	 */
	@SuppressWarnings("static-method")
	@Test
	public final void registerBundleByEnumClassInItalian()
	{
		ResourceBundleManager.setLocale(italian);
		ResourceBundleManager.register(BundleDemeterBase.class);

		Assert.assertTrue(ResourceBundleManager.getLocale().getLanguage() == italian.getLanguage());
	}

	/**
	 * Test the extraction of a dummy message in english.
	 */
	@SuppressWarnings("static-method")
	@Test
	public final void retrieveDummyMessageInEnglish()
	{
		try
		{
			ResourceBundleManager.setLocale(english);

			Assert.assertTrue("Not the english dummy message!", Collator.getInstance(english).equals(ResourceBundleManager.getMessage(BundleDemeterBase.TestDummy), DUMMY_ENGLISH));

		}
		catch (final ResourceException e)
		{
			fail(e.getLocalizedMessage());
		}
	}

	/**
	 * Test the extraction of a dummy message in french.
	 */
	@SuppressWarnings("static-method")
	@Test
	public final void retrieveDummyMessageInFrench()
	{
		try
		{
			ResourceBundleManager.setLocale(french);

			Assert.assertTrue("Not the french dummy message!", Collator.getInstance(french).equals(ResourceBundleManager.getMessage(BundleDemeterBase.TestDummy), DUMMY_FRENCH));
		}
		catch (final ResourceException e)
		{
			fail(e.getLocalizedMessage());
		}
	}

	/**
	 * Test the extraction of a dummy message in german.
	 */
	@SuppressWarnings("static-method")
	@Test
	public final void retrieveDummyMessageInGerman()
	{
		try
		{
			ResourceBundleManager.setLocale(german);

			Assert.assertTrue("Not the german dummy message!", Collator.getInstance(german).equals(ResourceBundleManager.getMessage(BundleDemeterBase.TestDummy), DUMMY_GERMAN));
		}
		catch (final ResourceException e)
		{
			fail(e.getLocalizedMessage());
		}
	}

	/**
	 * Test the extraction of a dummy message in spanish.
	 */
	@SuppressWarnings("static-method")
	@Test
	public final void retrieveDummyMessageInSpanish()
	{
		try
		{
			ResourceBundleManager.setLocale(spanish);

			Assert.assertTrue("Not the spanish dummy message!", Collator.getInstance(spanish).equals(ResourceBundleManager.getMessage(BundleDemeterBase.TestDummy), DUMMY_SPANISH));
		}
		catch (final ResourceException e)
		{
			fail(e.getLocalizedMessage());
		}
	}

	/**
	 * Test the extraction of a dummy message in italian.
	 */
	@SuppressWarnings("static-method")
	@Test
	public final void retrieveDummyMessageInItalian()
	{
		try
		{
			ResourceBundleManager.setLocale(italian);

			Assert.assertTrue("Not the italian dummy message!", Collator.getInstance(italian).equals(ResourceBundleManager.getMessage(BundleDemeterBase.TestDummy), DUMMY_ITALIAN));
		}
		catch (final ResourceException e)
		{
			fail(e.getLocalizedMessage());
		}
	}
}