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

#include <osgEarth/Registry>
#include <osgEarth/FileUtils>
#include <osgEarthFeatures/FeatureSource>
#include <osgEarthFeatures/Filter>
#include <osgEarthFeatures/BufferFilter>
#include <osgEarthFeatures/ScaleFilter>
#include <osgEarthFeatures/GeometryUtils>
#include "PostGISFeatureOptions"
#include "FeatureCursorPostGIS"
#include "PostGisUtils"
#include <osg/Notify>
#include <osgDB/FileNameUtils>
#include <osgDB/FileUtils>
#include <list>
#include <memory>
#include <cassert>

#define LC "[PostGIS FeatureSource] "

using namespace osgEarth;
using namespace osgEarth::Features;
using namespace osgEarth::Drivers;


/**
 * A FeatureSource that reads features from an PostGIS driver.
 *
 * This FeatureSource does NOT support styling.
 */
class PostGISFeatureSource : public FeatureSource
{
public:
    PostGISFeatureSource( const PostGISFeatureOptions& options ) : FeatureSource( options ),
      _conn( 0L ),
      _options( options ),
      _featureCount(-1),
      _needsSync(false),
      _writable(false)
    {
        //nop
    }

    /** Destruct the object, cleaning up and PostGIS handles. */
    virtual ~PostGISFeatureSource()
    {       
        if ( _conn )
        {
            PQfinish( _conn );
            _conn = 0L;
        }
    }

    //override
    void initialize( const osgDB::Options* )
    {
        //nop
    }

    /** Called once at startup to create the profile for this feature set. Successful profile
        creation implies that the datasource opened succesfully. */
    const FeatureProfile* createFeatureProfile()
    {
        FeatureProfile* result = 0L;

        // see if we have a custom profile.
        osg::ref_ptr<const Profile> profile;
        if ( _options.profile().isSet() )
        {
            profile = Profile::create( *_options.profile() );
        }

        // attempt to open the dataset:
        int openMode = _options.openWrite().isSet() && _options.openWrite().value() ? 1 : 0;


        const std::string conninfo = (_options.host().isSet() ? "" : " host='"+_options.host().value()+"'")
                                   + (_options.port().isSet() ? "" : " port='"+_options.port().value()+"'")
                                   + " dbname='"+_options.dbname().value()+"'"
                                   + (_options.user().isSet() ? "" : " user='"+_options.user().value()+"'")
                                   + (_options.password().isSet() ? "" : " password='"+_options.password().value()+"'")
                                   ;
        _conn = PQconnectdb(conninfo.c_str());

        if ( CONNECTION_OK == PQstatus(_conn) )
        {                                     
            // build a spatial index if requested.
            if ( _options.buildSpatialIndex() == true )
            {
                OE_INFO << LC << "Building spatial index for " << _options.table().value() << std::endl;
                const std::string query( "CREATE INDEX osgearth_index ON "+_options.table().value()
                        +" USING GIST ("+_options.geometryColumn().value()+"); VACUUM ANALYZE" );
                OE_DEBUG << LC << "SQL: " << query << std::endl;
                PostGisUtils::QueryResult res( _conn, query );
                if (!res)
                {
                    OE_INFO << LC << "failed to create spatial index: " << res.error() << std::endl;
                }
            }

            GeoExtent extent;

            // if the user provided a profile, user that:
            if ( profile.valid() )
            {
                result = new FeatureProfile( profile->getExtent() );
            }
            else
            {
                // extract the SRS and Extent:                
                PostGisUtils::QueryResult res( _conn,
                    "WITH a AS (SELECT ST_SRID("+_options.geometryColumn().value()
                    +") AS srid FROM "+_options.table().value()+" LIMIT 1)"
                    +" SELECT auth_name,auth_srid FROM spatial_ref_sys  s, a WHERE s.srid=a.srid");
                if (res)
                {
                    assert( 1 == PQntuples( res.get() ) );
                    const std::string authName( PQgetvalue( res.get() , 0, 0) ); 
                    const std::string authSrid( PQgetvalue( res.get() , 0, 1) ); 
                    const std::string proj4text( PQgetvalue( res.get() , 0, 2) ); 

                    osg::ref_ptr<SpatialReference> srs = SpatialReference::create( "EPSG" == authName ? "epsg:"+authSrid : proj4text );

                    if ( srs.valid() )
                    {
                        // extract the full extent of the layer:
                        PostGisUtils::QueryResult res2( _conn, "SELECT ST_Extent(" + _options.geometryColumn().value() + ") AS table_extent FROM " + _options.table().value() );
                        if (res2)
                        {
                            assert( 1 == PQntuples( res2.get() ) );
                            PostGisUtils::Lwgeom geom( PQgetvalue( res2.get() , 0, 0), PostGisUtils::Lwgeom::WKT() );
                            const GBOX * box = lwgeom_get_bbox(geom.get());
                            GeoExtent extent( srs.get(), box->xmin, box->ymin, box->xmax, box->ymax );
                            
                            // got enough info to make the profile!
                            result = new FeatureProfile( extent );
                        }
                        else
                        {
                            OE_INFO << LC << "failed to get layer spacial extent: " << res2.error() << std::endl;
                        }
                    }
                }
                else
                {
                    OE_INFO << LC << "failed to create spatial index: " << res.error() << std::endl;
                }
            }


            //Get the feature count
            {
                PostGisUtils::QueryResult res( _conn, "SELECT count(*) FROM " + _options.table().value() );
                if (res)
                {
                    _featureCount = atoi( PQgetvalue( res.get() , 0, 0) );
                }
                else
                {
                    OE_INFO << LC << "failed to get feature count: " << res.error() << std::endl;
                }
            }


            // init Schema
            {
                PostGisUtils::QueryResult res( _conn, "SELECT column_name, data_type FROM information_schema.columns WHERE table_name='" + _options.table().value() + "'") ;
                const int numCol = PQntuples( res.get() );
                for (int i = 0; i < numCol; i++)
                {
                    const std::string name( PQgetvalue( res.get(), i, 0 ) );
                    const std::string type( PQgetvalue( res.get(), i, 1 ) );
                    if      ( "text" == type ) _schema[ name ] =  ATTRTYPE_STRING;
                    else if ( "double precision" == type) _schema[ name ] =  ATTRTYPE_DOUBLE;
                    else if ( "integer" == type) _schema[ name ] =  ATTRTYPE_INT;
                    else                  _schema[ name ] =  ATTRTYPE_UNSPECIFIED;
                }
            }


            //!@todo find the actual geomtry type 
            _geometryType = Geometry::TYPE_POLYGON;
            //_geometryType = Geometry::TYPE_LINESTRING;
            //_geometryType = Geometry::TYPE_RING;
            //_geometryType = Geometry::TYPE_POINTSET;
            //_geometryType = Geometry::TYPE_MULTI;
        }
        else
        {
            OE_INFO << LC << "failed to open database with \"" << conninfo << "\"" << std::endl;
            PQfinish( _conn );
            _conn = 0L;
        }

        return result;
    }


    //override
    FeatureCursor* createFeatureCursor( const Symbology::Query& query )
    {
        if ( _conn )
        {
            return new FeatureCursorPostGIS( 
                _conn,
                _options.table().value(),
                _options.geometryColumn().value(),
                _options.featureIdColumn().value(),
                this,
                getFeatureProfile(),
                query, 
                _options.filters() );
        }
        else
        {
            return 0L;
        }
    }

    virtual bool deleteFeature(FeatureID fid)
    {
        if ( isWritable() ) OE_INFO << LC << "not implemented" << std::endl;
        return false;
    }

    virtual int getFeatureCount() const
    {
        return _featureCount;
    }

    virtual Feature* getFeature( FeatureID fid )
    {
        Feature* result = NULL;

        if ( !isBlacklisted(fid) )
        {
            OE_INFO << LC << "not implemented" << std::endl;
            assert(false && bool("not implemented"));
            //if (handle)
            //{
            //    const FeatureProfile* p = getFeatureProfile();
            //    const SpatialReference* srs = p ? p->getSRS() : 0L;
            //    result = OgrUtils::createFeature( handle, srs );
            //    OGR_F_Destroy( handle );
            //}
        }
        return result;
    }

    virtual bool isWritable() const
    {
        return _writable;
    }

    const FeatureSchema& getSchema() const
    {
        return _schema;
    } 

    virtual bool insertFeature(Feature* feature)
    {
        if ( isWritable() ) OE_INFO << LC << "not implemented" << std::endl;
        return false;
    }

    virtual osgEarth::Symbology::Geometry::Type getGeometryType() const
    {
        return _geometryType;
    }

protected:

    // parses an explicit WKT geometry string into a Geometry.
    Symbology::Geometry* parseGeometry( const Config& geomConf )
    {
        return GeometryUtils::geometryFromWKT( geomConf.value() );
    }

    // read the WKT geometry from a URL, then parse into a Geometry.
    Symbology::Geometry* parseGeometryUrl( const std::string& geomUrl, const osgDB::Options* dbOptions )
    {
        ReadResult r = URI(geomUrl).readString( dbOptions );
        if ( r.succeeded() )
        {
            Config conf( "geometry", r.getString() );
            return parseGeometry( conf );
        }
        return 0L;
    }


private:
    PGconn * _conn;
    
    const PostGISFeatureOptions _options;
    int _featureCount;
    bool _needsSync;
    bool _writable;
    FeatureSchema _schema;
    Geometry::Type _geometryType;
};


class PostGISFeatureSourceFactory : public FeatureSourceDriver
{
public:
    PostGISFeatureSourceFactory()
    {
        supportsExtension( "osgearth_feature_ogr", "PostGIS feature driver for osgEarth" );
    }

    virtual const char* className()
    {
        return "PostGIS Feature Reader";
    }

    virtual ReadResult readObject(const std::string& file_name, const Options* options) const
    {
        if ( !acceptsExtension(osgDB::getLowerCaseFileExtension( file_name )))
            return ReadResult::FILE_NOT_HANDLED;

        return ReadResult( new PostGISFeatureSource( getFeatureSourceOptions(options) ) );
    }
};

REGISTER_OSGPLUGIN(osgearth_feature_ogr, PostGISFeatureSourceFactory)

