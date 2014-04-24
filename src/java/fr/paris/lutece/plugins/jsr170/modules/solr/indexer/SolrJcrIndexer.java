/*
 * Copyright (c) 2002-2014, Mairie de Paris
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
package fr.paris.lutece.plugins.jsr170.modules.solr.indexer;

import java.io.IOException;
import java.io.Reader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Document;

import fr.paris.lutece.plugins.jcr.authentication.JsrUser;
import fr.paris.lutece.plugins.jcr.business.admin.AdminJcrHome;
import fr.paris.lutece.plugins.jcr.business.admin.AdminView;
import fr.paris.lutece.plugins.jcr.business.admin.AdminWorkspace;
import fr.paris.lutece.plugins.jcr.business.portlet.Jsr170Portlet;
import fr.paris.lutece.plugins.jcr.business.portlet.Jsr170PortletHome;
import fr.paris.lutece.plugins.jcr.service.JcrPlugin;
import fr.paris.lutece.plugins.jcr.service.jcrsearch.JcrSearchItem;
import fr.paris.lutece.plugins.jcr.service.search.IndexerNodeAction;
import fr.paris.lutece.plugins.jcr.service.search.JcrIndexer;
import fr.paris.lutece.plugins.jcr.util.JcrIndexerUtils;
import fr.paris.lutece.plugins.jsr170.modules.solr.business.SolrRepositoryFileHome;
import fr.paris.lutece.plugins.search.solr.business.field.Field;
import fr.paris.lutece.plugins.search.solr.indexer.SolrIndexer;
import fr.paris.lutece.plugins.search.solr.indexer.SolrIndexerService;
import fr.paris.lutece.plugins.search.solr.indexer.SolrItem;
import fr.paris.lutece.plugins.search.solr.util.SolrConstants;
import fr.paris.lutece.portal.business.page.Page;
import fr.paris.lutece.portal.business.page.PageHome;
import fr.paris.lutece.portal.business.portlet.Portlet;
import fr.paris.lutece.portal.business.portlet.PortletTypeHome;
import fr.paris.lutece.portal.service.plugin.Plugin;
import fr.paris.lutece.portal.service.plugin.PluginService;
import fr.paris.lutece.portal.service.search.SearchItem;
import fr.paris.lutece.portal.service.util.AppLogService;
import fr.paris.lutece.portal.service.util.AppPropertiesService;


/**
 * The indexer service for Solr.
 *
 */
public class SolrJcrIndexer implements SolrIndexer
{
    private static final String PROPERTY_DESCRIPTION = "jsr170-solr.indexer.description";
    private static final String PROPERTY_NAME = "jsr170-solr.indexer.name";
    private static final String PROPERTY_VERSION = "jsr170-solr.indexer.version";
    private static final String PROPERTY_INDEXER_ENABLE = "jsr170-solr.indexer.enable";
    private static final String PROPERTY_MIME_TYPE_LABEL = "jsr170-solr.indexer.mimeType.label";
    private static final String PROPERTY_MIME_TYPE_DESCRIPTION = "jsr170-solr.indexer.mimeType.description";
    private static final List<String> LIST_RESSOURCES_NAME = new ArrayList<String>(  );
    private static final String JCR_INDEXATION_ERROR = "[SolrJcrIndexer] An error occured during the indexation of the portlet number ";
    
    // Site name
    private static JcrIndexer _indexer = new JcrIndexer(  );
    private static Map<String, ISolrItemBuilder> _mapActions = new HashMap<String, ISolrItemBuilder>(  );

    public SolrJcrIndexer(  )
    {
        super(  );

        LIST_RESSOURCES_NAME.add( JcrIndexerUtils.CONSTANT_TYPE_RESOURCE );

        initMapBuilder(  );
    }

    /**
     * Init the actions map
     */
    private void initMapBuilder(  )
    {
        _mapActions.put( SearchItem.FIELD_CONTENTS,
            new ISolrItemBuilder(  )
            {
                public void action( org.apache.lucene.document.Field field, SolrItem item )
                    throws IOException
                {
                    Reader content = field.readerValue(  );

                    if ( content != null )
                    {
                        String strContent = new String( IOUtils.toByteArray( content ) );
                        item.setContent( strContent );
                    }
                }
            } );
        _mapActions.put( SearchItem.FIELD_DATE,
            new ISolrItemBuilder(  )
            {
                public void action( org.apache.lucene.document.Field field, SolrItem item )
                {
                    try
                    {
                        item.setDate( DateTools.stringToDate( field.stringValue(  ) ) );
                    }
                    catch ( ParseException e )
                    {
                        throw new RuntimeException( e );
                    }
                }
            } );
        _mapActions.put( SearchItem.FIELD_DOCUMENT_PORTLET_ID,
            new ISolrItemBuilder(  )
            {
                public void action( org.apache.lucene.document.Field field, SolrItem item )
                {
                    item.setDocPortletId( field.stringValue(  ) );
                }
            } );
        _mapActions.put( SearchItem.FIELD_METADATA,
            new ISolrItemBuilder(  )
            {
                public void action( org.apache.lucene.document.Field field, SolrItem item )
                {
                    item.setMetadata( field.stringValue(  ) );
                }
            } );
        _mapActions.put( SearchItem.FIELD_ROLE,
            new ISolrItemBuilder(  )
            {
                public void action( org.apache.lucene.document.Field field, SolrItem item )
                {
                    item.setRole( field.stringValue(  ) );
                }
            } );
        _mapActions.put( SearchItem.FIELD_SUMMARY,
            new ISolrItemBuilder(  )
            {
                public void action( org.apache.lucene.document.Field field, SolrItem item )
                {
                    item.setSummary( field.stringValue(  ) );
                }
            } );
        _mapActions.put( SearchItem.FIELD_TITLE,
            new ISolrItemBuilder(  )
            {
                public void action( org.apache.lucene.document.Field field, SolrItem item )
                {
                    item.setTitle( field.stringValue(  ) );
                }
            } );
        _mapActions.put( SearchItem.FIELD_TYPE,
            new ISolrItemBuilder(  )
            {
                public void action( org.apache.lucene.document.Field field, SolrItem item )
                {
                    item.setType( field.stringValue(  ) );
                }
            } );
        _mapActions.put( SearchItem.FIELD_UID,
            new ISolrItemBuilder(  )
            {
                public void action( org.apache.lucene.document.Field field, SolrItem item )
                {
                    item.setUid( field.stringValue(  ) );
                }
            } );
        _mapActions.put( SearchItem.FIELD_URL,
            new ISolrItemBuilder(  )
            {
                public void action( org.apache.lucene.document.Field field, SolrItem item )
                {
                    item.setUrl( field.stringValue(  ) );
                }
            } );
        _mapActions.put( JcrSearchItem.FIELD_MIME_TYPE,
            new ISolrItemBuilder(  )
            {
                public void action( org.apache.lucene.document.Field field, SolrItem item )
                {
                    item.addDynamicField( JcrSearchItem.FIELD_MIME_TYPE, field.stringValue(  ) );
                }
            } );
    }

    /**
         * {@inheritDoc}
         */
    public String getDescription(  )
    {
        return AppPropertiesService.getProperty( PROPERTY_DESCRIPTION );
    }

    /**
         * {@inheritDoc}
         */
    public String getName(  )
    {
        return AppPropertiesService.getProperty( PROPERTY_NAME );
    }

    /**
         * {@inheritDoc}
         */
    public String getVersion(  )
    {
        return AppPropertiesService.getProperty( PROPERTY_VERSION );
    }

    /**
     * {@inheritDoc}
     */
    public List<String> indexDocuments(  )
    {
        Plugin plugin = PluginService.getPlugin( JcrPlugin.PLUGIN_NAME );

        // definition of the comparator used in the collected files
        // thus there can't be two Documents with the same UID Field 
        final Comparator<Document> documentComparator = new Comparator<Document>(  )
            {
                public int compare( Document o1, Document o2 )
                {
                    if ( ( o1 != null ) && ( o2 != null ) )
                    {
                        return o1.getField( SearchItem.FIELD_UID ).stringValue(  )
                                 .compareTo( o2.getField( SearchItem.FIELD_UID ).stringValue(  ) );
                    }
                    else if ( ( o1 == null ) && ( o2 != null ) )
                    {
                        return -1;
                    }
                    else if ( ( o1 != null ) && ( o2 == null ) )
                    {
                        return 1;
                    }

                    return 0;
                }
            };

        Jsr170Portlet jsr170Portlet;
        int defaultView;
        AdminView view;
        String strRole;
        List<String> lstErrors = new ArrayList<String>(  );

        for ( Portlet portlet : Jsr170PortletHome.findByType( PortletTypeHome.getPortletTypeId( 
                    Jsr170PortletHome.class.getName(  ) ) ) )
        {
            jsr170Portlet = Jsr170PortletHome.getInstance(  ).findByPortletId( portlet.getId(  ) );

            defaultView = jsr170Portlet.getDefaultView(  );

            Page page = PageHome.findByPrimaryKey( portlet.getPageId(  ) );
            strRole = page.getRole(  );

            if ( defaultView > 0 )
            {
                view = AdminJcrHome.getInstance(  ).findViewById( defaultView, plugin );

                final AdminWorkspace adminWorkspace = AdminJcrHome.getInstance(  )
                                                                  .findWorkspaceById( view.getWorkspaceId(  ), plugin );

                try
                {
                    SolrRepositoryFileHome.getSolrInstance(  )
                                          .doRecursive( adminWorkspace, view, view.getPath(  ),
                        new IndexerNodeAction( documentComparator, JcrPlugin.PLUGIN_NAME, adminWorkspace, strRole ),
                        new JsrUser( adminWorkspace.getUser(  ) ) );
                }
                catch ( Exception e )
                {
                	lstErrors.add( SolrIndexerService.buildErrorMessage( e ) );
    				AppLogService.error( JCR_INDEXATION_ERROR + portlet.getId(  ), e );
                }
            }
        }

        return lstErrors;
    }

    /**
         * {@inheritDoc}
         */
    public List<SolrItem> getDocuments( String strIdDocument )
    {
        List<SolrItem> lstSolrItems = new ArrayList<SolrItem>(  );
        List<Document> lstLuceneDocuments = _indexer.getDocuments( strIdDocument );

        if ( lstLuceneDocuments != null )
        {
            for ( Document luceneDoc : lstLuceneDocuments )
            {
                SolrItem item = luceneDocument2SolrItem( luceneDoc );

                if ( item != null )
                {
                    lstSolrItems.add( item );
                }
            }
        }

        return lstSolrItems;
    }

    /**
     * Converts a lucene document into a {@link SolrItem} object
     * @param luceneDocument the document to convert
     * @return the {@link SolrItem} object of the lucene document. Returns null if the lucene document is null or has no fields.
     */
    public SolrItem luceneDocument2SolrItem( Document luceneDocument )
    {
        if ( ( luceneDocument == null ) || ( luceneDocument.getFields(  ) == null ) ||
                luceneDocument.getFields(  ).isEmpty(  ) )
        {
            return null;
        }

        SolrItem item = new SolrItem(  );

        for ( Object object : luceneDocument.getFields(  ) )
        {
            org.apache.lucene.document.Field field = (org.apache.lucene.document.Field) object;

            ISolrItemBuilder builder = _mapActions.get( field.name(  ) );

            // Add the field into the SolrItem object
            try
            {
                builder.action( field, item );
            }
            catch ( IOException e )
            {
                AppLogService.error( e.getMessage(  ), e );

                return null;
            }

            // Add Solr specific attribute
            item.setSite( SolrIndexerService.getWebAppName(  ) );
        }

        return item;
    }

    /**
         * {@inheritDoc}
         */
    public boolean isEnable(  )
    {
        return "true".equalsIgnoreCase( AppPropertiesService.getProperty( PROPERTY_INDEXER_ENABLE ) );
    }

    /**
         * {@inheritDoc}
         */
    public List<Field> getAdditionalFields(  )
    {
        Field mimeTypeField = new Field(  );
        mimeTypeField.setIsFacet( true );
        mimeTypeField.setLabel( PROPERTY_MIME_TYPE_LABEL );
        mimeTypeField.setDescription( PROPERTY_MIME_TYPE_DESCRIPTION );
        mimeTypeField.setName( JcrSearchItem.FIELD_MIME_TYPE + SolrItem.DYNAMIC_STRING_FIELD_SUFFIX );

        List<Field> lstFields = new ArrayList<Field>(  );
        lstFields.add( mimeTypeField );

        return lstFields;
    }

    /**
         * {@inheritDoc}
         */
    public List<String> getResourcesName(  )
    {
        return LIST_RESSOURCES_NAME;
    }

    /**
     * {@inheritDoc}
     */
    public String getResourceUid( String strResourceId, String strResourceType )
    {
        StringBuffer sb = new StringBuffer( strResourceId );
        sb.append( SolrConstants.CONSTANT_UNDERSCORE ).append( JcrIndexer.SHORT_NAME );

        return sb.toString(  );
    }

    /**
     *
     * ISolrItemBuilder
     *
     */
    private interface ISolrItemBuilder
    {
        /**
         * Fill the {@link SolrItem} object with the {@link org.apache.lucene.document.Field} object
         * @param field the Field object
         * @param item the SolrItem to fill
         * @throws IOException
         */
        public void action( org.apache.lucene.document.Field field, SolrItem item )
            throws IOException;
    }
}
