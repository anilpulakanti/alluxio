namespace java alluxio.thrift

include "common.thrift"
include "exception.thrift"

service MuKeyValueWorkerClientService extends common.AlluxioService {
  /**
   * Looks up a key in the given kv store path.
   */
  binary get( /** the path of the kv store */ 1: string path,
      /** binary of the key */ 2: binary key)
    throws (1: exception.AlluxioTException e, 2: exception.ThriftIOException ioe)

   /**
   * Puts a key in the given kv store path.
   */
  void put( /** the path of the kv store */ 1: string path,
      /** binary of the key */ 2: binary key, /** binary of the value */ 3: binary value)
    throws (1: exception.AlluxioTException e, 2: exception.ThriftIOException ioe)
    
  /**
   * Deletes a key in the given kv store path.
   */
  void deleteKey( /** the path of the kv store */ 1: string path,
      /** binary of the key */ 2: binary key)
    throws (1: exception.AlluxioTException e, 2: exception.ThriftIOException ioe)

}
