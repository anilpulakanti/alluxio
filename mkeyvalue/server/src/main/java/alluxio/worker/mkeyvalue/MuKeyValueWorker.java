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

/**
 *
 */
package alluxio.worker.mkeyvalue;

import alluxio.worker.AbstractWorker;

import org.apache.thrift.TProcessor;

import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * @author anil
 *
 */
public final class MuKeyValueWorker extends AbstractWorker {

  protected MuKeyValueWorker(ExecutorService executorService) {
    super(executorService);
    // TODO Auto-generated constructor stub
  }

  @Override
  public Map<String, TProcessor> getServices() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void start() throws Exception {
    // TODO Auto-generated method stub

  }

  @Override
  public void stop() throws Exception {
    // TODO Auto-generated method stub

  }

}
