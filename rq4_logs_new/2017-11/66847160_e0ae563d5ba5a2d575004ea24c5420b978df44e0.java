/*
 * Copyright (c) 2002-2017, Mairie de Paris
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *  1. Redistributions of source code must retain the above copyright notice
 *     and the following disclaimer.
 *
 *  2. Redistributions in binary form must reproduce the above copyright notice
 *     and the following disclaimer in the documentation and/or other materials
 *     provided with the distribution.
 *
 *  3. Neither the name of 'Mairie de Paris' nor 'Lutece' nor the names of its
 *     contributors may be used to endorse or promote products derived from
 *     this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * License 1.0
 */
package fr.paris.lutece.test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.Test;
import org.xml.sax.SAXException;

public class UtilsTest
{

    @Test
    public void testValidateHtmlFragment( ) throws IOException, SAXException
    {
        Utils.validateHtmlFragment( "<div>hello world</div>" );
    }

    @Test
    public void testValidateHtml( ) throws IOException, SAXException
    {
        Utils.validateHtml( "<!DOCTYPE html><title>junit</title><div>hello world</div>" );
    }

    @Test
    public void testValidateHtmlFragmentInvalidElementNotClosed( ) throws IOException, SAXException
    {
        boolean failed = false;
        try
        {
            Utils.validateHtmlFragment( "<a href='test?foo&bar'>link" );
        }
        catch ( AssertionError e )
        {
            failed = true;
        }
        assertTrue( "Should have failed", failed );
    }

    @Test
    public void testValidateHtmlInvalidElementNotClosed( ) throws IOException, SAXException
    {
        boolean failed = false;
        try
        {
            Utils.validateHtml( "<!DOCTYPE html><title>junit</title><a href='test?foo&bar'>link" );
        }
        catch ( AssertionError e )
        {
            failed = true;
        }
        assertTrue( "Should have failed", failed );
    }

    @Test
    public void testValidateHtmlFragmentWarning( ) throws IOException, SAXException
    {
        boolean failed = false;
        try
        {
            Utils.validateHtmlFragment( "<button role='button'>foo</button>" );
        }
        catch ( AssertionError e )
        {
            failed = true;
        }
        assertTrue( "Should have failed", failed );
    }

    @Test
    public void testValidateHtmlWarning( ) throws IOException, SAXException
    {
        boolean failed = false;
        try
        {
            Utils.validateHtml( "<!DOCTYPE html><title>junit</title><button role='button'>foo</button>" );
        }
        catch ( AssertionError e )
        {
            failed = true;
        }
        assertTrue( "Should have failed", failed );
    }

    @Test
    public void testValidateHtmlFragmentWarningIgnore( ) throws IOException, SAXException
    {
        try
        {
            Utils.validateHtmlFragment( "<button role='button'>foo</button>", false );
        }
        catch ( AssertionError e )
        {
            fail( "Should not have failed, but got " + e.getMessage( ) );
        }
    }

    @Test
    public void testValidateHtmlWarningIgnore( ) throws IOException, SAXException
    {
        try
        {
            Utils.validateHtml( "<!DOCTYPE html><title>junit</title><button role='button'>foo</button>", false );
        }
        catch ( AssertionError e )
        {
            fail( "Should not have failed, but got " + e.getMessage( ) );
        }
    }
}