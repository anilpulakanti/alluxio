namespace java alluxio.thrift

include "common.thrift"
include "exception.thrift"

/**
 * Information about a key-value partition.
 */
struct PartitionInfo {
  1: string path
  2: i64 mergeOffset
  3: common.WorkerNetAddress address
}

/**
 * This interface contains key-value master service endpoints for Alluxio clients.
 */
service MuKeyValueMasterClientService extends common.AlluxioService {

  /**
   * Creates a new key-value store on master.
   */
  void createStore( /** the path of the store */  1: string path)
    throws (1: exception.AlluxioTException e)

  /**
   * Gets the partition information for the key-value store at the given filesystem path.
   */
  list<PartitionInfo> getPartitionInfo( /** the path of the store */ 1: string path)
    throws (1: exception.AlluxioTException e)

  /**
   * Deletes a completed key-value store.
   */
  void deleteStore( /** the path of the store */ 1: string path)
    throws (1: exception.AlluxioTException e, 2: exception.ThriftIOException ioe)

  /**
   * Renames a completed key-value store.
   */
  void renameStore( /** the old path of the store */ 1: string oldPath,
      /**the new path of the store*/ 2:string newPath)
    throws (1: exception.AlluxioTException e, 2: exception.ThriftIOException ioe)

  /**
   * Merges one completed key-value store to another completed key-value store.
   */
  void mergeStore( /** the path of the store to be merged */ 1: string fromPath,
      /** the path of the store to be merged to */ 2: string toPath)
    throws (1: exception.AlluxioTException e, 2: exception.ThriftIOException ioe)
}
