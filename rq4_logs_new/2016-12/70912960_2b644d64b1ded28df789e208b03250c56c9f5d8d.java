/*
 * Copyright(c) 2016 - Heliosphere Corp.
 * ---------------------------------------------------------------------------
 * This file is part of the Heliosphere's project which is licensed under the 
 * Apache license version 2 and use is subject to license terms.
 * You should have received a copy of the license with the project's artifact
 * binaries and/or sources.
 * 
 * License can be consulted at http://www.apache.org/licenses/LICENSE-2.0
 * ---------------------------------------------------------------------------
 */
package com.heliosphere.demeter.base.resource.test;

import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.heliosphere.demeter.base.resource.IResource;
import com.heliosphere.demeter.base.resource.Resource;
import com.heliosphere.demeter.base.resource.ResourceException;

/**
 * Test class for the {@link Resource} class.
 * <hr>
 * @author <a href="mailto:christophe.resse@gmail.com">Resse Christophe - Heliosphere</a>
 * @version 1.0.0
 */
public class ResourceTest
{
	/**
	 * Initialization of the test cases.
	 * <p>
	 * @throws Exception In case an error occurs during the initialization.
	 */
	@BeforeClass
	public static final void setUpBeforeClass() throws Exception
	{
		// Empty
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
	@SuppressWarnings("static-method")
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
	 * Creates a resource based on a file with an absolute path name.
	 */
	@SuppressWarnings({ "static-method", "nls" })
	@Test
	public final void createAbsoluteResourceFile()
	{
		try
		{
			IResource holder = new Resource("/unit/path/test/test-file.txt");
			String pathname = holder.getFile().getAbsolutePath();

			IResource resource = new Resource(pathname);

			Assert.assertTrue(resource != null);
			Assert.assertTrue(resource.getFile() != null);
			Assert.assertTrue(resource.getFile().isFile());
			Assert.assertTrue(resource.getFile().isAbsolute());
			Assert.assertTrue(resource.getFile().exists());
		}
		catch (ResourceException e)
		{
			fail(e.getMessage());
		}
	}

	/**
	 * Creates a resource based on a file with a relative path name.
	 */
	@SuppressWarnings({ "static-method", "nls" })
	@Test
	public final void createRelativeResourceFile()
	{
		try
		{
			IResource resource = new Resource("/unit/path/test/test-file.txt");

			Assert.assertTrue(resource != null);
			Assert.assertTrue(resource.getFile() != null);
			Assert.assertTrue(resource.getFile().exists());
			Assert.assertTrue(resource.getFile().isFile());
			Assert.assertTrue(resource.getFile().isAbsolute());
		}
		catch (ResourceException e)
		{
			fail(e.getMessage());
		}
	}

	/**
	 * Creates a resource based on a folder with an absolute path name.
	 */
	@SuppressWarnings({ "static-method", "nls" })
	@Test
	public final void createAbsoluteResourceFolder()
	{
		try
		{
			IResource holder = new Resource("/unit/path/test/test-file.txt");
			String pathname = holder.getFile().getParent();

			IResource resource = new Resource(pathname);

			Assert.assertTrue(resource != null);
			Assert.assertTrue(resource.getFile() != null);
			Assert.assertTrue(resource.getFile().isDirectory());
			Assert.assertTrue(resource.getFile().isAbsolute());
			Assert.assertTrue(resource.getFile().exists());
		}
		catch (ResourceException e)
		{
			fail(e.getMessage());
		}
	}

	/**
	 * Creates a resource based on a folder with a relative path name.
	 */
	@SuppressWarnings({ "static-method", "nls" })
	@Test
	public final void createRelativeResourceFolder()
	{
		try
		{
			IResource resource = new Resource("/unit/path/test");

			Assert.assertTrue(resource != null);
			Assert.assertTrue(resource.getFile() != null);
			Assert.assertTrue(resource.getFile().isDirectory());
			Assert.assertTrue(resource.getFile().isAbsolute());
			Assert.assertTrue(resource.getFile().exists());
		}
		catch (ResourceException e)
		{
			fail(e.getMessage());
		}
	}
}