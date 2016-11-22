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

import alluxio.AbstractClient;
import alluxio.Constants;
import alluxio.exception.AlluxioException;
import alluxio.thrift.AlluxioService;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.MuKeyValueWorkerClientService;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.WorkerNetAddress;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Client for talking to a mkey-value worker server.
 *
 * Since {@link alluxio.thrift.MuKeyValueWorkerClientService.Client} is not thread safe, this class
 * has to guarantee thread safety.
 */
@ThreadSafe
public final class MuKeyValueWorkerClient extends AbstractClient {

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Creates a {@link MuKeyValueWorkerClient}.
   *
   * @param workerNetAddress location of the worker to connect to
   */
  public MuKeyValueWorkerClient(WorkerNetAddress workerNetAddress) {
    super(NetworkAddressUtils.getRpcPortSocketAddress(workerNetAddress), "mkey-value-worker");
  }

  private MuKeyValueWorkerClientService.Client mClient = null;

  @Override
  protected AlluxioService.Client getClient() {
    return mClient;
  }

  @Override
  protected String getServiceName() {
    return Constants.MU_KEY_VALUE_WORKER_CLIENT_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.MU_KEY_VALUE_WORKER_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() throws IOException {
    mClient = new MuKeyValueWorkerClientService.Client(mProtocol);
  }

  /**
   * Gets the value of a given {@code key} from a specific mkey-value store partition.
   *
   * @param path The path of the partition
   * @param key the key to get the value for
   * @return ByteBuffer of value, or null if not found
   * @throws IOException if an I/O error occurs
   * @throws AlluxioException if an Alluxio error occurs
   */
  public synchronized ByteBuffer get(final String path, final ByteBuffer key)
      throws IOException, AlluxioException {
    return retryRPC(new RpcCallableThrowsAlluxioTException<ByteBuffer>() {
      @Override
      public ByteBuffer call() throws AlluxioTException, TException {
        return mClient.get(path, key);
      }
    });
  }

  /**
   * Puts the value of a given {@code key} to a specific mkey-value store partition.
   *
   * @param path The path of the partition
   * @param key the key to get the value for
   * @param value the value of the key
   * @throws IOException if an I/O error occurs
   * @throws AlluxioException if an Alluxio error occurs
   */
  public synchronized void put(final String path, final ByteBuffer key, final ByteBuffer value)
      throws IOException, AlluxioException {
    retryRPC(new RpcCallableThrowsAlluxioTException<Void>() {
      @Override
      public Void call() throws AlluxioTException, TException {
        mClient.put(path, key, value);
        return null;
      }
    });
  }

  /**
   * Deletes the key of a given {@code key} from a specific mkey-value store partition.
   *
   * @param path The path of the partition
   * @param key the key to get the value for
   * @throws IOException if an I/O error occurs
   * @throws AlluxioException if an Alluxio error occurs
   */
  public synchronized void delete(final String path, final ByteBuffer key)
      throws IOException, AlluxioException {
    retryRPC(new RpcCallableThrowsAlluxioTException<Void>() {
      @Override
      public Void call() throws AlluxioTException, TException {
        mClient.deleteKey(path, key);
        return null;
      }
    });
  }

}
