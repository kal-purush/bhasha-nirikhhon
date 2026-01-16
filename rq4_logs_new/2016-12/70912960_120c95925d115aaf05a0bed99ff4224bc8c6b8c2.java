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
package com.heliosphere.demeter.base.resource;

/**
 * Provides an abstract implementation of a file.
 * <hr>
 * @author <a href="mailto:christophe.resse@gmail.com">Resse Christophe - Heliosphere</a>
 * @version 1.0.0
 */
public class AbstractFile implements IFile
{
	/**
	 * Default serialization identifier.
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Resource representing this file.
	 */
	private IResource resource;

	/**
	 * Creates a new abstract file.
	 * <hr>
	 * @param pathname File pathname.
	 */
	public AbstractFile(String pathname)
	{
		resource = new Resource(pathname);
	}

	@Override
	public final IResource getResource()
	{
		return resource;
	}
}