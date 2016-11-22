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

import alluxio.annotation.PublicApi;
import alluxio.exception.AlluxioException;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Interface for readers and writers which accesses mkey-value stores in Alluxio.
 */
@PublicApi
public interface MuKeyValueStore {

  /**
   * Gets the value associated with {@code key}, returns null if not found.
   *
   * @param key key to get, cannot be null
   * @return value associated with the given key, or null if not found
   * @throws IOException if non-Alluxio error occurs
   * @throws AlluxioException if Alluxio error occurs
   */
  ByteBuffer get(ByteBuffer key) throws IOException, AlluxioException;

  /**
   * Adds a key and its associated value to this store.
   *
   * @param key key to put, cannot be null
   * @param value value to put, cannot be null
   * @throws IOException if non-Alluxio error occurs
   * @throws AlluxioException if Alluxio error occurs
   */
  void put(ByteBuffer key, ByteBuffer value) throws IOException, AlluxioException;

  /**
   * Deletes a key and its associated value in this store.
   *
   * @param key key to delete, cannot be null
   * @throws IOException if non-Alluxio error occurs
   * @throws AlluxioException if Alluxio error occurs
   */
  void delete(ByteBuffer key) throws IOException, AlluxioException;

}
