# pgv-me
###### _Spreading embedded data integration etl_
This projects builds an executable JAR - therefore multi-platform.

This jar holds some Pentaho Data Integration libs as well as one ktr file. 

When clicked/executed, this bundle executes the embedded ktr file.

It was built by modifying the example project available at http://ci.pentaho.com/job/kettle-sdk-embedding-samples/.

### Building

The eclipse project has relative paths only. However, there is one of the properties from `build.properties` that is an absolute path. The purpose of this path is to receive a copy of the generated JAR for testing purposes.