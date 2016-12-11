/**
 * 
 */
package alluxio.worker.mkeyvalue;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.worker.Worker;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.WorkerFactory;

/**
 * @author viswanath2
 *
 */
public class MuKeyValueWorkerFactory implements WorkerFactory {
	 private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

	  /**
	   * Constructs a new {@link KeyValueWorkerFactory}.
	   */
	  public MuKeyValueWorkerFactory() {}

	  @Override
	  public MuKeyValueWorker create(List<? extends Worker> workers) {
	    if (!Configuration.getBoolean(PropertyKey.KEY_VALUE_ENABLED)) {
	      return null;
	    }
	    LOG.info("Creating Worker {} ", MuKeyValueWorker.class.getName());

	    for (Worker worker : workers) {
	      if (worker instanceof BlockWorker) {
	        LOG.info("{} is created", MuKeyValueWorker.class.getName());
	        return new MuKeyValueWorker(((BlockWorker) worker));
	      }
	    }
	    LOG.error("Fail to create {} due to missing {}", MuKeyValueWorker.class.getName(),
	        BlockWorker.class.getName());
	    return null;
	  }
}
