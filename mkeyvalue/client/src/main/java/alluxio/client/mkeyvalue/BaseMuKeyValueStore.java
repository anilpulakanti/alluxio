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

package alluxio.client.mkeyvalue;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.ClientContext;
import alluxio.exception.AlluxioException;
import alluxio.thrift.MuPartitionInfo;
import alluxio.thrift.WorkerNetAddress;
import alluxio.wire.ThriftUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Default implementation of {@link MuKeyValueStore} to access an Alluxio mkey-value store.
 */
@NotThreadSafe
public class BaseMuKeyValueStore implements MuKeyValueStore {

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final InetSocketAddress mMasterAddress = ClientContext.getMasterAddress();
  private final MuKeyValueMasterClient mMasterClient;

  /** A list of partitions of the store. */
  private final List<MuPartitionInfo> mPartitions;

  /**
   * Constructs a {@link BaseMuKeyValueStore} instance.
   *
   * @param uri URI of the mkey-value store
   * @throws IOException if non-Alluxio error occurs
   * @throws AlluxioException if Alluxio error occurs
   */
  BaseMuKeyValueStore(AlluxioURI uri) throws IOException, AlluxioException {
    LOG.info("Create MuKeyValueStore for {}", uri);
    mMasterClient = new MuKeyValueMasterClient(mMasterAddress);
    mPartitions = mMasterClient.getPartitionInfo(uri);
    mMasterClient.close();
    Preconditions.checkNotNull(mPartitions, "KeyValue store doesn't exist");
    Preconditions.checkState(mPartitions.size() != 0, "KeyValue store has no partitions");
  }

  @Override
  public ByteBuffer get(ByteBuffer key) throws IOException, AlluxioException {
    int partitionIndex = MuKeyValueHashFunction.computeHash(key) % mPartitions.size();
    WorkerNetAddress workerNetAddress = mPartitions.get(partitionIndex).getAddress();
    String path = mPartitions.get(partitionIndex).getPath();
    MuKeyValueWorkerClient workerClient =
        new MuKeyValueWorkerClient(ThriftUtils.fromThrift(workerNetAddress));
    ByteBuffer value = workerClient.get(path, key);
    workerClient.close();
    return value;
  }

  @Override
  public void put(ByteBuffer key, ByteBuffer value) throws IOException, AlluxioException {
    int partitionIndex = MuKeyValueHashFunction.computeHash(key) % mPartitions.size();
    WorkerNetAddress workerNetAddress = mPartitions.get(partitionIndex).getAddress();
    String path = mPartitions.get(partitionIndex).getPath();
    MuKeyValueWorkerClient workerClient =
        new MuKeyValueWorkerClient(ThriftUtils.fromThrift(workerNetAddress));
    workerClient.put(path, key, value);
    workerClient.close();
  }

  @Override
  public void delete(ByteBuffer key) throws IOException, AlluxioException {
    int partitionIndex = MuKeyValueHashFunction.computeHash(key) % mPartitions.size();
    WorkerNetAddress workerNetAddress = mPartitions.get(partitionIndex).getAddress();
    String path = mPartitions.get(partitionIndex).getPath();
    MuKeyValueWorkerClient workerClient =
        new MuKeyValueWorkerClient(ThriftUtils.fromThrift(workerNetAddress));
    workerClient.delete(path, key);
    workerClient.close();
  }

}
