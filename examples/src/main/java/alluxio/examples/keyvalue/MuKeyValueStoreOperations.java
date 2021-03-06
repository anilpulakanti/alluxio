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

package alluxio.examples.keyvalue;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.cli.CliUtils;
import alluxio.client.mkeyvalue.MuKeyValueStore;
import alluxio.client.mkeyvalue.MuKeyValueSystem;
import alluxio.util.io.BufferUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * This example illustrates how to create a mkey-value store, put key-value pairs into the store,
 * and read the store afterwards.
 */
public class MuKeyValueStoreOperations implements Callable<Boolean> {

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final int mPartitionLength = Constants.MB;
  private final int mNumKeyValuePairs = 1000;

  private AlluxioURI mStoreUri;
  private Map<ByteBuffer, ByteBuffer> mKeyValuePairs = new HashMap<>();

  /**
   * @param storeUri URI of the key-value store to write to, should not exist before
   * @throws Exception if the instance fails to be created
   */
  public MuKeyValueStoreOperations(AlluxioURI storeUri) throws Exception {
    mStoreUri = storeUri; 
  }

  @Override
  public Boolean call() throws Exception {
    Configuration.set(PropertyKey.KEY_VALUE_ENABLED, String.valueOf(true));
    Configuration.set(PropertyKey.KEY_VALUE_PARTITION_SIZE_BYTES_MAX,
        String.valueOf(mPartitionLength));

    MuKeyValueSystem kvs = MuKeyValueSystem.Factory.create();

    MuKeyValueStore store = kvs.createStore(mStoreUri);
    putKeyValuePairs(store);

    boolean pass = getKeyValuePairs(store);
    return true;
  }

  private void putKeyValuePairs(MuKeyValueStore writer) throws Exception {
    LOG.info("Putting key-value pairs...");
    // API: KeyValueStoreWriter#put
    for (int i = 0; i < mNumKeyValuePairs; i++) {
      // Keys are 0, 1, 2, etc.
      byte[] key = ByteBuffer.allocate(4).putInt(i).array();
      // Values are byte arrays of length {@link #mValueLength}.
      //int valueLength = mPartitionLength / 2;
      byte[] value = BufferUtils.getIncreasingByteArray(4);
      writer.put(ByteBuffer.wrap(key), ByteBuffer.wrap(value));
      mKeyValuePairs.put(ByteBuffer.wrap(key), ByteBuffer.wrap(value));
    }
  }

  private boolean getKeyValuePairs(MuKeyValueStore reader) throws Exception {
    LOG.info("Getting key-value pairs...");

    // API: KeyValueStoreReader#get
    for (Map.Entry<ByteBuffer, ByteBuffer> pair : mKeyValuePairs.entrySet()) {
      ByteBuffer expectedValue = pair.getValue();
      ByteBuffer gotValue = reader.get(pair.getKey());
      if (!expectedValue.equals(gotValue)) {
        LOG.error("The value returned from the key-value store iterator is unexpected");
        return false;
      }
    }

    return true;
  }

  /**
   * Starts in a command like {@code java -cp ALLUXIO_JAR CLASS_NAME <key-value store URI>}.
   *
   * @param args one argument, specifying the URI of the store to be created
   * @throws Exception if unexpected errors happen
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.out.println("Usage: java -cp " + RuntimeConstants.ALLUXIO_JAR + " "
          + MuKeyValueStoreOperations.class.getName() + " <key-value store URI>");
      System.exit(-1);
    }

    if (!Configuration.getBoolean(PropertyKey.KEY_VALUE_ENABLED)) {
      System.out.println("Alluxio key value service is disabled. To run this test, please set "
          + PropertyKey.KEY_VALUE_ENABLED + " to be true and restart the cluster.");
      System.exit(-1);
    }

    // TODO(binfan): the "run and exit" pattern shows up repeatedly in the code base and it might
    // make sense to add a utility function for it to CliUtils
    boolean result = CliUtils.runExample(new MuKeyValueStoreOperations(new AlluxioURI(args[0])));
    System.exit(result ? 0 : 1);
  }

}
