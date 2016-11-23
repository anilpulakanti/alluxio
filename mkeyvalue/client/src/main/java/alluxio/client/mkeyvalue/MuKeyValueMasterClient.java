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

import alluxio.AbstractMasterClient;
import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.exception.AlluxioException;
import alluxio.thrift.AlluxioService;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.MuKeyValueMasterClientService;
import alluxio.thrift.MuPartitionInfo;

import org.apache.thrift.TException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper for the thrift client to interact with the mkey-value master, used by Alluxio clients.
 * This wrapper provides thread safety, and retry mechanism.
 */
@ThreadSafe
public final class MuKeyValueMasterClient extends AbstractMasterClient {

  private MuKeyValueMasterClientService.Client mClient = null;

  /**
   * Creates a new key-value master client.
   *
   * @param masterAddress the master address
   */
  public MuKeyValueMasterClient(InetSocketAddress masterAddress) {
    super(masterAddress);
  }

  @Override
  protected AlluxioService.Client getClient() {
    return mClient;
  }

  @Override
  protected String getServiceName() {
    return Constants.MU_KEY_VALUE_MASTER_CLIENT_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.MU_KEY_VALUE_MASTER_CLIENT_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() throws IOException {
    mClient = new MuKeyValueMasterClientService.Client(mProtocol);
  }

  /**
   * Creates a new mkey-value store.
   *
   * @param path URI of the mkey-value store
   * @throws AlluxioException if an Alluxio error occurs
   * @throws IOException if an I/O error occurs
   */
  public synchronized void createStore(final AlluxioURI path) throws IOException, AlluxioException {
    retryRPC(new RpcCallableThrowsAlluxioTException<Void>() {
      @Override
      public Void call() throws AlluxioTException, TException {
        mClient.createStore(path.getPath());
        return null;
      }
    });
  }

  /**
   * Gets a list of partitions of a given mkey-value store.
   *
   * @param path URI of the mkey-value store
   * @return a list of partition information
   * @throws AlluxioException if an Alluxio error occurs
   * @throws IOException if an I/O error occurs
   */
  public synchronized List<MuPartitionInfo> getPartitionInfo(final AlluxioURI path)
      throws IOException, AlluxioException {
    return retryRPC(new RpcCallableThrowsAlluxioTException<List<MuPartitionInfo>>() {
      @Override
      public List<MuPartitionInfo> call() throws AlluxioTException, TException {
        return mClient.getPartitionInfo(path.getPath());
      }
    });
  }

  /**
   * Deletes a mkey-value store.
   *
   * @param path URI of the store
   * @throws IOException if non-Alluxio error occurs
   * @throws AlluxioException if other Alluxio error occurs
   */
  public synchronized void deleteStore(final AlluxioURI path) throws IOException, AlluxioException {
    retryRPC(new RpcCallableThrowsAlluxioTException<Void>() {
      @Override
      public Void call() throws AlluxioTException, TException {
        mClient.deleteStore(path.getPath());
        return null;
      }
    });
  }

  /**
   * Renames a mkey-value store.
   *
   * @param oldPath old URI of the store
   * @param newPath new URI of the store
   * @throws IOException if non-Alluxio error occurs
   * @throws AlluxioException if other Alluxio error occurs
   */
  public synchronized void renameStore(final AlluxioURI oldPath, final AlluxioURI newPath)
      throws IOException, AlluxioException {
    retryRPC(new RpcCallableThrowsAlluxioTException<Void>() {
      @Override
      public Void call() throws AlluxioTException, TException {
        mClient.renameStore(oldPath.getPath(), newPath.getPath());
        return null;
      }
    });
  }

  /**
   * Merges one mkey-value store to another mkey-value store.
   *
   * @param fromPath URI of the store to be merged
   * @param toPath URI of the store to be merged to
   */
  void mergeStore(final AlluxioURI fromPath, final AlluxioURI toPath)
      throws IOException, AlluxioException {
    retryRPC(new RpcCallableThrowsAlluxioTException<Void>() {
      @Override
      public Void call() throws AlluxioTException, TException {
        mClient.mergeStore(fromPath.getPath(), toPath.getPath());
        return null;
      }
    });
  }
}
