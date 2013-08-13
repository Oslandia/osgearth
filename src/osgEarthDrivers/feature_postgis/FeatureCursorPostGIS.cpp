/* -*-c++-*- */
/* osgEarth - Dynamic map generation toolkit for OpenSceneGraph
 * Copyright 2008-2013 Pelican Mapping
 * http://osgearth.org
 *
 * osgEarth is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */
#include "FeatureCursorPostGIS"
#include "PostGisUtils"
#include <osgEarthFeatures/Feature>
#include <osgEarth/Registry>
#include <algorithm>

#define LC "[FeatureCursorPostGIS] "

#define PostGIS_SCOPED_LOCK GDAL_SCOPED_LOCK

using namespace osgEarth;
using namespace osgEarth::Features;


FeatureCursorPostGIS::FeatureCursorPostGIS(PGconn *         conn,
                                   const std::string &      table,
                                   const std::string &      geometryColumn,
                                   const std::string &      featureIdColumn,
                                   const FeatureSource*     source,
                                   const FeatureProfile*    profile,
                                   const Symbology::Query&  query,
                                   const FeatureFilterList& filters )
{
    // create the query
    std::string expr;
    {
        std::string from = table;
        std::string geom = geometryColumn;
        
        // Or quote any layers containing spaces
        if (from.find(" ") != std::string::npos)
            from = "\"" + from + "\"";                    

        if (geom.find(" ") != std::string::npos)
            geom = "\"" + geom + "\"";                    

        // if there's a spatial extent in the query, build the spatial filter:
        std::string extend;
        if ( query.bounds().isSet() )
        {
            std::stringstream buf;
            buf << " " << geom << " && SetSRID('BOX3D(" 
                << std::setprecision(17) // to be on the safe side
                << query.bounds()->xMin() << " " << query.bounds()->yMin() << ","
                << query.bounds()->xMax() << " " << query.bounds()->yMax()
                << ")'::box3d,-1)";
            extend = buf.str();
        }

        if ( query.expression().isSet() )
        {
            // build the SQL: allow the Query to include either a full SQL statement or
            // just the WHERE clause.
            expr = query.expression().value();

            // if the expression is just a where clause, expand it into a complete SQL expression.
            std::string temp = expr;
            std::transform( temp.begin(), temp.end(), temp.begin(), ::tolower );
            if ( temp.find( "select" ) != 0 )
            {
                std::stringstream buf;
                buf << "SELECT * FROM " << from << " WHERE " << expr << (extend.empty() ? "" : " AND " + extend);
                std::string bufStr;
                bufStr = buf.str();
                expr = bufStr;
            }
        }
        else
        {
            std::stringstream buf;
            buf << "SELECT * FROM " << from << extend;
            expr = buf.str();
        }


        //Include the order by clause if it's set
        if (query.orderby().isSet())
        {                     
            std::string orderby = query.orderby().value();
            
            std::string temp = orderby;
            std::transform( temp.begin(), temp.end(), temp.begin(), ::tolower );

            if ( temp.find( "order by" ) != 0 )
            {                
                std::stringstream buf;
                buf << "ORDER BY " << orderby;                
                std::string bufStr;
                bufStr = buf.str();
                orderby = buf.str();
            }
            expr += (" " + orderby );
        }
    }


    OE_DEBUG << LC << "SQL: " << expr << std::endl;
    PostGisUtils::QueryResult res( conn, expr );

    if ( !res )
    {
        OE_INFO << LC << "failed to execute request"<< std::endl;
        return;
    }

    // populate the feature queue
    {
        const int featureIdIdx = PQfnumber(res.get(), featureIdColumn.c_str() );
        if ( featureIdIdx < 0 )
        {
            OE_INFO << LC << "failed to obtain feature id from '"<< featureIdColumn <<"'\n";
            return;
        }
        const int geomIdx = PQfnumber(res.get(), geometryColumn.c_str() );
        if ( geomIdx < 0 )
        {
            OE_INFO << LC << "failed to obtain geometry from '"<< geometryColumn <<"'\n";
            return;
        }
        FeatureList preProcessList;

        const int numFeatures = PQntuples( res.get() );

        for( unsigned i=0; i<numFeatures; i++ )
        {
            const char * wkb = PQgetvalue( res.get(), i, geomIdx );
            PostGisUtils::Lwgeom lwgeom( wkb, PostGisUtils::Lwgeom::WKB() );
            Symbology::Geometry* geom = NULL;
            //! @todo actually create the geometry
            //switch ( lwgeom.get()->type )
            //{
            //case POINTTYPE:
            //case LINETYPE:
            //case POLYGONTYPE:
            //    Symbology::Polygon poly( numPoints );

            //case TRIANGLETYPE:
            //case TINTYPE:
            //case POLYHEDRALSURFACETYPE:
            //case COLLECTIONTYPE:
            //case MULTIPOINTTYPE:
            //case MULTILINETYPE:
            //case MULTIPOLYGONTYPE:
            //case MULTISURFACETYPE:

            //case MULTICURVETYPE:
            //case CIRCSTRINGTYPE:
            //case COMPOUNDTYPE:
            //case CURVEPOLYTYPE:

            //}


            const int fid = atoi( PQgetvalue( res.get(), i, featureIdIdx));
            osg::ref_ptr<Feature> f = new Feature( geom, profile->getSRS(), Style(), fid );

            const int numCol = PQnfields( res.get() );
            for ( int c=0; c<numCol; c++)
            {
                if ( geomIdx == c ) continue;
                const std::string name( PQfname( res.get(), c ) );
                const std::string value( PQgetvalue( res.get(), i, c ) );
                f->set( name, value );
                
                //! @todo actually convert to int or double type
                //switch ( PQftype( res.get(), c ) )
            }



            if ( f.valid() && !source->isBlacklisted(f->getFID()) )
            {
                _queue.push( f );

                if ( filters.size() > 0 )
                    preProcessList.push_back( f.release() );
            }
        }

        // preprocess the features using the filter list:
        if ( preProcessList.size() > 0 )
        {
            FilterContext cx;
            cx.profile() = profile;

            for( FeatureFilterList::const_iterator i = filters.begin(); i != filters.end(); ++i )
            {
                FeatureFilter* filter = i->get();
                cx = filter->push( preProcessList, cx );
            }
        }
    }
}

FeatureCursorPostGIS::~FeatureCursorPostGIS()
{
    //nop
}

bool
FeatureCursorPostGIS::hasMore() const
{
    return  _queue.size() > 0;
}

Feature*
FeatureCursorPostGIS::nextFeature()
{
    if ( !hasMore() )
        return 0L;

    // do this in order to hold a reference to the feature we return, so the caller
    // doesn't have to. This lets us avoid requiring the caller to use a ref_ptr when 
    // simply iterating over the cursor, making the cursor move conventient to use.
    _lastFeatureReturned = _queue.front();
    _queue.pop();

    return _lastFeatureReturned.get();
}

