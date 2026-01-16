/*
 * Copyright (c) 2002-2020, City of Paris
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
package fr.paris.lutece.plugins.participatoryideation.web;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import fr.paris.lutece.portal.util.mvc.commons.annotations.View;
import fr.paris.lutece.portal.util.mvc.xpage.MVCApplication;
import fr.paris.lutece.portal.util.mvc.xpage.annotations.Controller;
import fr.paris.lutece.portal.web.LocalVariables;
import fr.paris.lutece.portal.web.xpages.XPage;

/**
 * This class provides page for searching proposals using solr requests.
 */
@Controller( xpageName = ProposalsXpage.PAGE_SOLR_PROPOSAL_SEARCH, pageTitleI18nKey = "participatoryideation.xpage.solrProposalSearch.pageTitle", pagePathI18nKey = "participatoryideation.xpage.solrProposalSearch.pagePathLabel" )
public class ProposalsXpage extends MVCApplication
{
	private static final long serialVersionUID = -7345931721575088511L;

	private static final Logger LOGGER = Logger.getLogger( ProposalsXpage.class );

    // Views
    public static final String PAGE_SOLR_PROPOSAL_SEARCH = "solrProposalSearch";

    // Parameters
    private static final String PARAMETER_FACET_QUERY = "fq";
    private static final String PARAMETER_SORT_ORDER = "sort_order";
    private static final String PARAMETER_SORT_NAME = "sort_name";
    private static final String PARAMETER_QUERY = "query";

    // Marks
    private static final String MARK_CONF = "conf";

    // Constants
    private static final String strSearchSolrUrl = "Portal.jsp?page=search-solr";
    private static final String strAmpSymbol = "&";
    private static final String strEqualSymbol = "=";

    @View( value = PAGE_SOLR_PROPOSAL_SEARCH, defaultView = true )
    public XPage solrProposalSearch( HttpServletRequest request ) throws ServletException, IOException
    {
        StringBuilder sbReq = new StringBuilder( strSearchSolrUrl );

        // Retrieve the search configuration
        String strConf = request.getParameter( MARK_CONF );
        if ( StringUtils.isNotBlank( strConf ) )
        {
            sbReq.append( strAmpSymbol + MARK_CONF + strEqualSymbol + strConf );
        }

        // Retrieve facet query from request
        String [ ] facetQuery = request.getParameterValues( PARAMETER_FACET_QUERY );
        if ( facetQuery != null )
        {
            for ( String strFacet : facetQuery )
            {
                sbReq.append( strAmpSymbol + "fq" + strEqualSymbol + strFacet );
            }
        }

        // Manage the sort of the query
        String strSortOrder = request.getParameter( PARAMETER_SORT_ORDER );
        if ( StringUtils.isNotBlank( strSortOrder ) )
        {
            sbReq.append( "&sort_order=" + strSortOrder );
        }
        else
        {
            sbReq.append( "&sort_order=asc" );
        }

        String strSortName = request.getParameter( PARAMETER_SORT_NAME );
        if ( StringUtils.isNotBlank( strSortName ) )
        {
            sbReq.append( "&sort_name=" + strSortName );
        }
        else
        {
            sbReq.append( "&sort_name=title" );
        }

        // Manage the query
        String strQuery = request.getParameter( PARAMETER_QUERY );
        if ( StringUtils.isNotBlank( strQuery ) )
        {
            sbReq.append( "&query=" + strQuery );
        }
        else
        {
            sbReq.append( "&query=*:*" );
        }

        LOGGER.debug( "Requête SOLR de date, redirection vers " + sbReq.toString( ) );

        UriComponents uriComponents = UriComponentsBuilder.fromUriString( sbReq.toString( ) ).build( );
        String strEncodedUri = uriComponents.encode( "UTF-8" ).toUriString( );

        // Make the redirection
        HttpServletResponse response = LocalVariables.getResponse( );
        response.sendRedirect( strEncodedUri );

        return new XPage( );
    }

}