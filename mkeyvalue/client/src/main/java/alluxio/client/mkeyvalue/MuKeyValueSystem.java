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
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;

import java.io.IOException;

/**
 * Client to access or create key-value stores in Alluxio.
 */
@PublicApi
public interface MuKeyValueSystem {

  /**
   * Factory for the {@link MuKeyValueSystem}.
   */
  final class Factory {
    private static MuKeyValueSystem sKeyValueSystem = null;

    private Factory() {} // to prevent initialization

    /**
     * @return a {@link MuKeyValueSystem} instance
     */
    public static synchronized MuKeyValueSystem create() {
      if (sKeyValueSystem == null) {
        sKeyValueSystem = new BaseMuKeyValueSystem();
      }
      return sKeyValueSystem;
    }
  }

  /**
   * Opens a mkey-value store and returns a store object.
   *
   * @param uri {@link AlluxioURI} to the store
   * @return {@link BaseKeyValueStoreReader} instance
   * @throws IOException if non-Alluxio error occurs
   * @throws AlluxioException if Alluxio error occurs
   */
  MuKeyValueStore openStore(AlluxioURI uri) throws IOException, AlluxioException;

  /**
   * Creates a new mkey-value store and returns a store object.
   *
   * @param uri {@link AlluxioURI} to the store
   * @return {@link BaseKeyValueStoreWriter} instance
   * @throws IOException if non-Alluxio error occurs
   * @throws AlluxioException if Alluxio error occurs
   */
  MuKeyValueStore createStore(AlluxioURI uri) throws IOException, AlluxioException;

  /**
   * Rename a mkey-value store.
   *
   * @param oldUri the old {@link AlluxioURI} to the store
   * @param newUri the new {@link AlluxioURI} to the store
   * @throws IOException if non-Alluxio error occurs
   * @throws AlluxioException if other Alluxio error occurs
   */
  void renameStore(AlluxioURI oldUri, AlluxioURI newUri) throws IOException, AlluxioException;

  /**
   * Deletes a mkey-value store.
   *
   * @param uri {@link AlluxioURI} to the store
   * @throws IOException if non-Alluxio error occurs
   * @throws InvalidPathException if the uri exists but is not a key-value store
   * @throws FileDoesNotExistException if the uri does not exist
   * @throws AlluxioException if other Alluxio error occurs
   */
  void deleteStore(AlluxioURI uri)
      throws IOException, InvalidPathException, FileDoesNotExistException, AlluxioException;

  /**
   * Merges one mkey-value store to another mkey-value store.
   *
   * If there are the same keys from both stores, they are merged too, for these keys, whose value
   * will be retrieved is undetermined.
   *
   * @param fromUri the {@link AlluxioURI} to the store to be merged
   * @param toUri the {@link AlluxioURI} to the store to be merged to
   * @throws IOException if non-Alluxio error occurs
   * @throws AlluxioException if other Alluxio error occurs
   */
  void mergeStore(AlluxioURI fromUri, AlluxioURI toUri) throws IOException, AlluxioException;
}
