/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.mkeyvalue;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.keyvalue.KeyValueStoreWriter;
import alluxio.client.keyvalue.KeyValueSystem;
import alluxio.clock.SystemClock;
import alluxio.exception.AccessControlException;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.master.AbstractMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.journal.Journal;
import alluxio.master.journal.JournalOutputStream;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.thrift.MuKeyValueMasterClientService;
import alluxio.thrift.PartitionInfo;
import alluxio.thrift.WorkerNetAddress;
import alluxio.util.IdUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.io.PathUtils;
import alluxio.wire.ThriftUtils;
import alluxio.wire.WorkerInfo;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The mkey-value master stores mkey-value store information in Alluxio, including the partitions of
 * each mkey-value store.
 */
@ThreadSafe
public final class MuKeyValueMaster extends AbstractMaster {

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private final FileSystemMaster mFileSystemMaster;

  /** Map from file id of a mkey-value store to the list of partitions in this store. */
  private final Map<Long, List<PartitionInfo>> mKVStoreToPartitions;

  /**
   * @param baseDirectory the base journal directory
   * @return the journal directory for this master
   */
  public static String getJournalDirectory(String baseDirectory) {
    return PathUtils.concatPath(baseDirectory, Constants.MU_KEY_VALUE_MASTER_NAME);
  }

  /**
   * @param fileSystemMaster handler to a {@link FileSystemMaster} to use for filesystem operations
   * @param journal a {@link Journal} to write journal entries to
   */
  public MuKeyValueMaster(FileSystemMaster fileSystemMaster,
      Journal journal) {
    super(journal, new SystemClock(), ExecutorServiceFactories
        .fixedThreadPoolExecutorServiceFactory(Constants.MU_KEY_VALUE_MASTER_NAME, 2));
    mFileSystemMaster = fileSystemMaster;
    mKVStoreToPartitions = new HashMap<>();
  }

  @Override
  public Map<String, TProcessor> getServices() {
    Map<String, TProcessor> services = new HashMap<>();
    services.put(Constants.MU_KEY_VALUE_MASTER_CLIENT_SERVICE_NAME,
        new MuKeyValueMasterClientService.Processor<>(new MuKeyValueMasterClientServiceHandler(this)));
    return services;
  }

  @Override
  public String getName() {
    return Constants.MU_KEY_VALUE_MASTER_NAME;
  }

  @Override
  public void processJournalEntry(JournalEntry entry) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void streamToJournalCheckpoint(JournalOutputStream outputStream) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void start(boolean isLeader) throws IOException {
    super.start(isLeader);
  }

  /**
   * Creates a new mkey-value store.
   *
   * @param path URI of the key-value store
   * @throws FileAlreadyExistsException if a key-value store URI exists
   * @throws InvalidPathException if the given path is invalid
   * @throws AccessControlException if permission checking fails
   */
  public synchronized void createStore(AlluxioURI path)
      throws FileAlreadyExistsException, InvalidPathException, AccessControlException {

    try {
      // Create this dir
      mFileSystemMaster.createDirectory(path, CreateDirectoryOptions.defaults().setRecursive(true));
    } catch (IOException e) {
      // TODO(binfan): Investigate why {@link mFileSystemMaster.createDirectory} throws IOException
      throw new InvalidPathException(
          String.format("Failed to createStore: can not create path %s", path), e);
    } catch (FileDoesNotExistException e) {
      // This should be impossible since we pass the recursive option into mkdir
      throw Throwables.propagate(e);
    }
    long fileId = mFileSystemMaster.getFileId(path);
    Preconditions.checkState(fileId != IdUtils.INVALID_FILE_ID);

    if (mKVStoreToPartitions.containsKey(fileId)) {
      // TODO(binfan): throw a better exception
      throw new FileAlreadyExistsException(String
          .format("Failed to createStore: KeyValueStore (fileId=%d) is already created", fileId));
    }

    List<WorkerInfo> workerInfoList = mFileSystemMaster.getWorkerInfoList();
    Preconditions.checkNotNull(workerInfoList);
    Preconditions.checkState(!workerInfoList.isEmpty(), "workerlist is empty");

    ArrayList<PartitionInfo> partitionList = new ArrayList<>();
    for(int i=0; i<workerInfoList.size(); i++){
      WorkerInfo workerInfo = workerInfoList.get(i);
      WorkerNetAddress workerNetAddress = ThriftUtils.toThrift(workerInfo.getAddress());
      AlluxioURI uri = getPartitionName(path.getPath(), i);
      try {
        createKVStore(uri);
      } catch (AlluxioException e) {
        throw new InvalidPathException(
            String.format("Failed to createStore: can not create path %s", path), e);
      } catch (IOException e) {
        throw new InvalidPathException(
            String.format("Failed to createStore: can not create path %s", path), e);
      }
      PartitionInfo partitionInfo = new PartitionInfo(uri.getPath(), 0, workerNetAddress);
      partitionList.add(partitionInfo);
    }
    mKVStoreToPartitions.put(fileId, partitionList);
  }

  private long getFileId(AlluxioURI uri)
      throws AccessControlException, FileDoesNotExistException, InvalidPathException {
    long fileId = mFileSystemMaster.getFileId(uri);
    if (fileId == IdUtils.INVALID_FILE_ID) {
      throw new FileDoesNotExistException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(uri));
    }
    return fileId;
  }

  /**
   * @return {@link AlluxioURI} to for a new partition file
   */
  private AlluxioURI getPartitionName(String path, int partIndex) {
    return new AlluxioURI(String.format("%s/part-%05d", path, partIndex));
  }

  private void createKVStore(AlluxioURI uri)
      throws AlluxioException, IOException {
    Preconditions.checkNotNull(uri);
    KeyValueSystem kvs = KeyValueSystem.Factory.create();
    KeyValueStoreWriter writer = kvs.createStore(uri);
    writer.close();
  }

  /**
   * Deletes a mkey-value store.
   *
   * @param uri {@link AlluxioURI} to the store
   * @throws IOException if non-Alluxio error occurs
   * @throws InvalidPathException if the uri exists but is not a key-value store
   * @throws FileDoesNotExistException if the uri does not exist
   * @throws AlluxioException if other Alluxio error occurs
   */
  public synchronized void deleteStore(AlluxioURI uri)
      throws IOException, InvalidPathException, FileDoesNotExistException, AlluxioException {
  //TODO: anil
  }

  /**
   * Renames one mkey-value store.
   *
   * @param oldUri the old {@link AlluxioURI} to the store
   * @param newUri the {@link AlluxioURI} to the store
   * @throws IOException if non-Alluxio error occurs
   * @throws AlluxioException if other Alluxio error occurs
   */
  public synchronized void renameStore(AlluxioURI oldUri, AlluxioURI newUri)
      throws IOException, AlluxioException {
  //TODO: anil
  }

  /**
   * Merges one mkey-value store to another mkey-value store.
   *
   * @param fromUri the {@link AlluxioURI} to the store to be merged
   * @param toUri the {@link AlluxioURI} to the store to be merged to
   * @throws IOException if non-Alluxio error occurs
   * @throws InvalidPathException if the uri exists but is not a key-value store
   * @throws FileDoesNotExistException if the uri does not exist
   * @throws AlluxioException if other Alluxio error occurs
   */
  public synchronized void mergeStore(AlluxioURI fromUri, AlluxioURI toUri)
      throws IOException, FileDoesNotExistException, InvalidPathException, AlluxioException {
    //TODO: anil
  }

  /**
   * Gets a list of partitions of a given mkey-value store.
   *
   * @param path URI of the key-value store
   * @return a list of partition information
   * @throws FileDoesNotExistException if the key-value store URI does not exists
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the path is invalid
   */
  public synchronized List<PartitionInfo> getPartitionInfo(AlluxioURI path)
      throws FileDoesNotExistException, AccessControlException, InvalidPathException {
    long fileId = getFileId(path);
    List<PartitionInfo> partitions = mKVStoreToPartitions.get(fileId);
    return partitions;
  }
}
