/*
 * Copyright (c) 2002-2009, Mairie de Paris
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
package fr.paris.lutece.plugins.jsr170.modules.solr.business;

import fr.paris.lutece.plugins.jcr.business.INodeAction;
import fr.paris.lutece.plugins.jcr.business.IRepositoryFile;
import fr.paris.lutece.plugins.jcr.business.IRepositoryFileDAO;
import fr.paris.lutece.plugins.jcr.business.RepositoryFileHome;
import fr.paris.lutece.plugins.jcr.business.admin.AdminWorkspace;
import fr.paris.lutece.plugins.jsr170.modules.solr.indexer.SolrJcrIndexer;
import fr.paris.lutece.plugins.search.solr.indexer.SolrIndexerService;
import fr.paris.lutece.plugins.search.solr.indexer.SolrItem;
import fr.paris.lutece.portal.service.spring.SpringContextService;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;

import java.io.IOException;

import java.util.Collection;


/**
 * Home class for JCR repository operations with Solr
 *
 */
public class SolrRepositoryFileHome extends RepositoryFileHome
{
    private static SolrRepositoryFileHome _singletonSolr;
    private SolrJcrIndexer _solrJcrIndexer = (SolrJcrIndexer) SpringContextService.getBean( 
            "jsr170-solr.solrDocIndexer" );

    /**
     * Returns the instance of the singleton
     * @return The instance of the singleton
     */
    public static SolrRepositoryFileHome getSolrInstance(  )
    {
        SolrRepositoryFileHome singleton = _singletonSolr;

        if ( singleton == null )
        {
            singleton = new SolrRepositoryFileHome(  );
            _singletonSolr = singleton;
        }

        return singleton;
    }

    /**
    * @param <T> the type of result elements
    * @param <L> the collection type
    * @param adminWorkspace the workspace
    * @param parentFile the file to star from
    * @param action the action to perform on each node
    * @return a new list of type <L>
    */
    @Override
    protected void doRecursive( AdminWorkspace adminWorkspace, IRepositoryFile parentFile,
        INodeAction<Document, Collection<Document>> action )
    {
        Document result = action.doAction( parentFile );

        if ( result != null )
        {
            try
            {
                SolrItem solrItem = _solrJcrIndexer.luceneDocument2SolrItem( result );

                if ( ( solrItem != null ) && ( solrItem.getContent(  ) != null ) )
                {
                    SolrIndexerService.write( solrItem );
                }
            }
            catch ( CorruptIndexException e )
            {
                e.printStackTrace(  );
            }
            catch ( IOException e )
            {
                e.printStackTrace(  );
            }
        }

        IRepositoryFileDAO dao = getIRepositoryFileDAO( adminWorkspace.getJcrType(  ) );

        for ( IRepositoryFile file : dao.listFiles( adminWorkspace.getName(  ), parentFile.getAbsolutePath(  ) ) )
        {
            result = action.doAction( file );

            if ( result != null )
            {
                try
                {
                    SolrItem solrItem = _solrJcrIndexer.luceneDocument2SolrItem( result );

                    if ( ( solrItem != null ) && ( solrItem.getContent(  ) != null ) )
                    {
                        SolrIndexerService.write( solrItem );
                    }
                }
                catch ( CorruptIndexException e )
                {
                    e.printStackTrace(  );
                }
                catch ( IOException e )
                {
                    e.printStackTrace(  );
                }
            }

            if ( file.isDirectory(  ) )
            {
                doRecursive( adminWorkspace, file, action );
            }
        }
    }
}
