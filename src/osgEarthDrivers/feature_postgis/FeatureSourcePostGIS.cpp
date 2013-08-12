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
#include <osg/Notify>
#include <osgDB/FileNameUtils>
#include <osgDB/FileUtils>
#include <list>
#include <libpq-fe.h>

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
    // utility class mainly for RAII of PGresult
    struct QueryResult
    {
        PostGISQuery( PGconn * conn, const std::string & query )
            : _res( PQexec( conn, query.c_str() ) )
            , _error( PQresultErrorMessage(_res) )
        {
            //nop
        }

        ~PostGISQuery() 
        { 
            PQclear(_res);
        }

        operator bool() const { return _error.empty(); }

        PGresult * get(){ return _res; }

        const std::string & erro() const { return _error; }

    private:
        PGresult _res;
        const std::string _error;
    };

    //utility class for RAII of LWGEOM
    struct Lwgeom
    {
        Lwgeom( const char * wkt )
            : _geom( lwgeom_from_wkt(wkt, LW_PARSER_CHECK_NONE) )
        {}
        LWGEOM * get(){ return _geom; }
        ~Lwgeom()
        {
            lwgeom_free(_geom);
        }
    private:
        LWGEOM * _geom;

    }

    PostGISFeatureSource( const PostGISFeatureOptions& options ) : FeatureSource( options ),
      _conn( 0L ),
      _layerHandle( 0L ),
      _layerIndex( 0 ),
      _ogrDriverHandle( 0L ),
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


        const std::string conninfo = (_options.host().empty() ? "" : " host='"+_options.host()+"'")
                                   + (_options.port().empty() ? "" : " port='"+_options.port()+"'")
                                   + " dbname='"+_options.dbname()+"'"
                                   + (_options.user().empty() ? "" : " user='"+_options.user()+"'")
                                   + (_options.password().empty() ? "" : " password='"+_options.password()+"'")
                                   ;
        _conn = PQconnectdb(conninfo.c_str());

        if ( CONNECTION_OK == PQstatus(_conn) )
        {                                     
            // build a spatial index if requested.
            if ( _options.buildSpatialIndex() == true )
            {
                OE_INFO << LC << "Building spatial index for " << _options.table() << std::endl;
                const std::string query( "CREATE INDEX osgearth_index ON "+_options.table()
                        +" USING GIST ("._options.geometryColumn()+"); VACUUM ANALYZE" );
                OE_DEBUG << LC << "SQL: " << query << std::endl;
                QueryResult res( _conn, query );
                if (!res)
                {
                    OE_INFO << LC << "failed to create spatial index: " << res.error() << std::endl;
                }
            }

int PQntuples(const PGresult *res);
int PQnfields(const PGresult *res);
char *PQfname(const PGresult *res, int column_number);
char *PQgetvalue(const PGresult *res, int row_number, int column_number);



            GeoExtent extent;

            // if the user provided a profile, user that:
            if ( profile.valid() )
            {
                result = new FeatureProfile( profile->getExtent() );
            }
            else
            {
                // extract the SRS and Extent:                
                QueryResult res( _conn,
                    "WITH a AS (SELECT ST_SRID("+_options.geometryColumn()
                    +") AS srid FROM "+_options.table()+" LIMIT 1)"
                    +" SELECT auth_name,auth_srid FROM spatial_ref_sys  s, a WHERE s.srid=a.srid")
                if (sridRes)
                {
                    assert( 1 == PQntuples( res.get() ) );
                    const std::string authName( PQgetvalue( res.get() , 0, 0) ); 
                    const std::string authSrid( PQgetvalue( res.get() , 0, 1) ); 
                    const std::string proj4text( PQgetvalue( res.get() , 0, 2) ); 

                    osg::ref_ptr<SpatialReference> srs = SpatialReference::create( "EPSG" == authName ? "epsg:"+authSrid : proj4text );

                    if ( srs.valid() )
                    {
                        // extract the full extent of the layer:
                        QueryResult res2( _conn, "SELECT ST_Extent(" + _options.geometryColumn() + ") AS table_extent FROM " + _options.table() );
                        if (res2)
                        {
                            assert( 1 == PQntuples( res2.get() ) );
                            Lwgeom geom( PQgetvalue( res2.get() , 0, 0) );
                            const GBOX * box = lwgeom_get_bbox(geom);
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
            QueryResult res( _conn, "SELECT count(*) FROM " + _options.table() );
            if (res)
            {
                _featureCount = atoi( PQgetvalue( res2.get() , 0, 0) );
            }
            else
            {
                OE_INFO << LC << "failed to get feature count: " << res.error() << std::endl;
            }

            // init Schema
            {
                OGRFeatureDefnH layerDef =  OGR_L_GetLayerDefn( _layerHandle );
                for (int i = 0; i < OGR_FD_GetFieldCount( layerDef ); i++)
                {
Oid PQftype(const PGresult *res, int column_number);

                    OGRFieldDefnH fieldDef = OGR_FD_GetFieldDefn( layerDef, i );
                    std::string name;
                    name = std::string( OGR_Fld_GetNameRef( fieldDef ) );
                    OGRFieldType ogrType = OGR_Fld_GetType( fieldDef );
                    {
                        switch (type)
                        {
                        case OFTString: _schema[ name ] =  ATTRTYPE_STRING;
                        case OFTReal: _schema[ name ] = ATTRTYPE_DOUBLE;
                        case OFTInteger: _schema[ name ] = ATTRTYPE_INT;
                        default: _schema[ name ] = ATTRTYPE_UNSPECIFIED;
                        }        
                    }
                }
            }

            OGRwkbGeometryType wkbType = OGR_FD_GetGeomType( OGR_L_GetLayerDefn( _layerHandle ) );
            if (
                wkbType == wkbPolygon ||
                wkbType == wkbPolygon25D )
            {
                _geometryType = Geometry::TYPE_POLYGON;
            }
            else if (
                wkbType == wkbLineString ||
                wkbType == wkbLineString25D )
            {
                _geometryType = Geometry::TYPE_LINESTRING;
            }
            else if (
                wkbType == wkbLinearRing )
            {
                _geometryType = Geometry::TYPE_RING;
            }
            else if ( 
                wkbType == wkbPoint ||
                wkbType == wkbPoint25D )
            {
                _geometryType = Geometry::TYPE_POINTSET;
            }
            else if (
                wkbType == wkbGeometryCollection ||
                wkbType == wkbGeometryCollection25D ||
                wkbType == wkbMultiPoint ||
                wkbType == wkbMultiPoint25D ||
                wkbType == wkbMultiLineString ||
                wkbType == wkbMultiLineString25D ||
                wkbType == wkbMultiPolygon ||
                wkbType == wkbMultiPolygon25D )
            {
                _geometryType = Geometry::TYPE_MULTI;
            }
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
        if ( _geometry.valid() )
        {
            return new GeometryFeatureCursor(
                _geometry.get(),
                getFeatureProfile(),
                _options.filters() );
                //getFilters() );
        }
        else
        {
            OGR_SCOPED_LOCK;

            // Each cursor requires its own DS handle so that multi-threaded access will work.
            // The cursor impl will dispose of the new DS handle.

            OGRDataSourceH dsHandle = OGROpenShared( _source.c_str(), 0, &_ogrDriverHandle );
            if ( dsHandle )
            {
                OGRLayerH layerHandle = OGR_DS_GetLayer( dsHandle, _layerIndex );

                return new FeatureCursorOGR( 
                    dsHandle,
                    layerHandle, 
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
    }

    virtual bool deleteFeature(FeatureID fid)
    {
        if (_writable && _layerHandle)
        {
            if (OGR_L_DeleteFeature( _layerHandle, fid ) == OGRERR_NONE)
            {
                _needsSync = true;
                return true;
            }            
        }
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
            OGR_SCOPED_LOCK;
            OGRFeatureH handle = OGR_L_GetFeature( _layerHandle, fid);
            if (handle)
            {
                const FeatureProfile* p = getFeatureProfile();
                const SpatialReference* srs = p ? p->getSRS() : 0L;
                result = OgrUtils::createFeature( handle, srs );
                OGR_F_Destroy( handle );
            }
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
        OGR_SCOPED_LOCK;
        OGRFeatureH feature_handle = OGR_F_Create( OGR_L_GetLayerDefn( _layerHandle ) );
        if ( feature_handle )
        {
            const AttributeTable& attrs = feature->getAttrs();

            // assign the attributes:
            int num_fields = OGR_F_GetFieldCount( feature_handle );
            for( int i=0; i<num_fields; i++ )
            {
                OGRFieldDefnH field_handle_ref = OGR_F_GetFieldDefnRef( feature_handle, i );
                std::string name = OGR_Fld_GetNameRef( field_handle_ref );
                int field_index = OGR_F_GetFieldIndex( feature_handle, name.c_str() );

                AttributeTable::const_iterator a = attrs.find( toLower(name) );
                if ( a != attrs.end() )
                {
                    switch( OGR_Fld_GetType(field_handle_ref) )
                    {
                    case OFTInteger:
                        OGR_F_SetFieldInteger( feature_handle, field_index, a->second.getInt(0) );
                        break;
                    case OFTReal:
                        OGR_F_SetFieldDouble( feature_handle, field_index, a->second.getDouble(0.0) );
                        break;
                    case OFTString:
                        OGR_F_SetFieldString( feature_handle, field_index, a->second.getString().c_str() );
                        break;
                    default:break;
                    }
                }
            }

            // assign the geometry:
            OGRFeatureDefnH def = ::OGR_L_GetLayerDefn( _layerHandle );

            OGRwkbGeometryType reported_type = OGR_FD_GetGeomType( def );

            OGRGeometryH ogr_geometry = OgrUtils::createOgrGeometry( feature->getGeometry(), reported_type );
            if ( OGR_F_SetGeometryDirectly( feature_handle, ogr_geometry ) != OGRERR_NONE )
            {
                OE_WARN << LC << "OGR_F_SetGeometryDirectly failed!" << std::endl;
            }

            if ( OGR_L_CreateFeature( _layerHandle, feature_handle ) != OGRERR_NONE )
            {
                //TODO: handle error better
                OE_WARN << LC << "OGR_L_CreateFeature failed!" << std::endl;
                OGR_F_Destroy( feature_handle );
                return false;
            }

            // clean up the feature
            OGR_F_Destroy( feature_handle );
        }
        else
        {
            //TODO: handle error better
            OE_WARN << LC << "OGR_F_Create failed." << std::endl;
            return false;
        }

        dirty();

        return true;
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
    std::string _source;
    PGconn * _conn;

    OGRLayerH _layerHandle;
    unsigned int _layerIndex;
    OGRSFDriverH _ogrDriverHandle;
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

