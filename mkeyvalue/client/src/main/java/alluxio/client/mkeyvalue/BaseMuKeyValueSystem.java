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
import alluxio.annotation.PublicApi;
import alluxio.client.ClientContext;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.PreconditionMessage;

import com.google.common.base.Preconditions;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Default implementation of the {@link MuKeyValueSystem} interface. Developers can extend this
 * class instead of implementing the interface. This implementation reads and writes key-value data
 * through {@link BaseMuKeyValueStore}.
 */
@PublicApi
@ThreadSafe
public final class BaseMuKeyValueSystem implements MuKeyValueSystem {
  private final MuKeyValueMasterClient mMasterClient =
      new MuKeyValueMasterClient(ClientContext.getMasterAddress());

  /**
   * Constructs a new {@link BaseMuKeyValueSystem}.
   */
  public BaseMuKeyValueSystem() {}

  @Override
  public MuKeyValueStore openStore(AlluxioURI uri) throws IOException, AlluxioException {
    Preconditions.checkNotNull(uri, PreconditionMessage.URI_KEY_VALUE_STORE_NULL);
    return new BaseMuKeyValueStore(uri);
  }

  @Override
  public MuKeyValueStore createStore(AlluxioURI uri) throws IOException, AlluxioException {
    Preconditions.checkNotNull(uri, PreconditionMessage.URI_KEY_VALUE_STORE_NULL);
    mMasterClient.createStore(uri);
    return new BaseMuKeyValueStore(uri);
  }

  @Override
  public void deleteStore(AlluxioURI uri)
      throws IOException, InvalidPathException, FileDoesNotExistException, AlluxioException {
    mMasterClient.deleteStore(uri);
  }

  @Override
  public void renameStore(AlluxioURI oldUri, AlluxioURI newUri)
      throws IOException, AlluxioException {
    mMasterClient.renameStore(oldUri, newUri);
  }

  @Override
  public void mergeStore(AlluxioURI fromUri, AlluxioURI toUri)
      throws IOException, AlluxioException {
    mMasterClient.mergeStore(fromUri, toUri);
  }
}
