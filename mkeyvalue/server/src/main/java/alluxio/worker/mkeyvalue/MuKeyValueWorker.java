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

import alluxio.Constants;
import alluxio.thrift.MuKeyValueWorkerClientService;
import alluxio.util.ThreadFactoryUtils;
import alluxio.worker.AbstractWorker;
import alluxio.worker.block.BlockWorker;


import org.apache.thrift.TProcessor;

import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;
//import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author anil
 *
 */
public final class MuKeyValueWorker extends AbstractWorker {

  private final BlockWorker mBlockWorker;
	  /** Logic for handling key-value RPC requests. */
  private final MuKeyValueWorkerClientServiceHandler mMuKeyValueServiceHandler;	
	
	
  /*protected MuKeyValueWorker(ExecutorService executorService) {
    super(executorService);
    // TODO Auto-generated constructor stub
  }*/

  public MuKeyValueWorker(BlockWorker blockWorker) {
	    // TODO(binfan): figure out do we really need thread pool for key-value worker (and for what)
	    super(Executors.newFixedThreadPool(1,
	        ThreadFactoryUtils.build("keyvalue-worker-heartbeat-%d", true)));
	    mBlockWorker = Preconditions.checkNotNull(blockWorker);
	    mMuKeyValueServiceHandler = new MuKeyValueWorkerClientServiceHandler(blockWorker);
	  //  Merge mmergeProcess = new Merge(mMuKeyValueServiceHandler.getmPartitionMap());
	   // mmergeProcess.run();
	  }
  
  
  @Override
  public Map<String, TProcessor> getServices() {
	    Map<String, TProcessor> services = new HashMap<>();
	    services.put(Constants.MU_KEY_VALUE_WORKER_CLIENT_SERVICE_NAME,
	        new MuKeyValueWorkerClientService.Processor<>(mMuKeyValueServiceHandler));
	    return services;
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
