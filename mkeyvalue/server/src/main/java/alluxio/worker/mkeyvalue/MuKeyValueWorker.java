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
