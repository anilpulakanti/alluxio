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

import alluxio.util.io.BufferUtils;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Hashfunction used by the client to figure out the right partition for the key.
 */
public class MuKeyValueHashFunction {

  MuKeyValueHashFunction() {}

  /**
   * Computes hash for the key.
   *
   * @param key key to compute hash
   * @return hash value for the key
   */
  public static int computeHash(ByteBuffer key) {
    byte[] bytes = BufferUtils.newByteArrayFromByteBuffer(key);
    return computeHash(bytes);
  }

  /**
   * Computes hash for the key.
   *
   * @param key key to compute hash
   * @return hash value for the key
   */
  public static int computeHash(byte[] key) {
    int num = Arrays.hashCode(key);
    if (num < 0) {
      num = -1 * num;
    }
    return num;
  }
}
