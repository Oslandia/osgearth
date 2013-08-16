SET(TARGET_SRC
    FeatureSourcePostGIS.cpp
    FeatureCursorPostGIS.cpp
    PostGisUtils.cpp
)

SET(TARGET_H
    FeatureCursorPostGIS    
    PostGISFeatureOptions
    PostGisUtils
)

INCLUDE_DIRECTORIES( ${PostgreSQL_INCLUDE_DIR} ${LWGEOM_INCLUDE_DIR} )
SET(TARGET_COMMON_LIBRARIES ${TARGET_COMMON_LIBRARIES} osgEarthFeatures osgEarthSymbology)
SET(TARGET_LIBRARIES_VARS PostgreSQL_LIBRARY LWGEOM_LIBRARY)
SETUP_PLUGIN(osgearth_feature_postgis)


# to install public driver includes:
SET(LIB_NAME feature_postgis)
SET(LIB_PUBLIC_HEADERS ${TARGET_H})
INCLUDE(ModuleInstallOsgEarthDriverIncludes OPTIONAL)
