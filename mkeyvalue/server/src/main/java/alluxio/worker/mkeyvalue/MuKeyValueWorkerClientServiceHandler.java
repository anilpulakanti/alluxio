/**
 * 
 */
package alluxio.worker.mkeyvalue;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;

import org.apache.thrift.TException;

import com.google.common.base.Preconditions;

import alluxio.Constants;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.MuKeyValueWorkerClientService;
import alluxio.thrift.ThriftIOException;
import alluxio.worker.block.BlockWorker;

import java.util.*;

/**
 * @author viswanath
 *
 */
public final class MuKeyValueWorkerClientServiceHandler implements MuKeyValueWorkerClientService.Iface  {

	private final BlockWorker mBlockWorker;
	/**
	 * @param blockWorker the {@link BlockWorker}
	 */
	private Map<String,MuKVWorkerPartitionInfo> mPartitionMap = null; 
	
	public Map<String, MuKVWorkerPartitionInfo> getmPartitionMap() {
		return mPartitionMap;
	}
	public void setmPartitionMap(Map<String, MuKVWorkerPartitionInfo> mPartitionMap) {
		this.mPartitionMap = mPartitionMap;
	}
	
	public MuKeyValueWorkerClientServiceHandler(BlockWorker blockWorker) {
	   // mBlockWorker = Preconditions.checkNotNull(blockWorker);
		mBlockWorker = null;
		mPartitionMap = new HashMap<>();
	  }
	@Override
	public long getServiceVersion() throws TException {
		// TODO Auto-generated method stub
		return Constants.MU_KEY_VALUE_WORKER_SERVICE_VERSION;
	}

	//Contains the map from the path of the KV store to the Indirection Column
	//and the current Merge Offset
	
	TailPageFileOutputStream mBufferedWriter;
																						
	
	@Override
	public ByteBuffer get(String path, ByteBuffer key) throws AlluxioTException, ThriftIOException, TException {
		System.out.println("In get function: Path is "+path);
		if(isPathValid(path)) {
		 
			if(!mPartitionMap.containsKey(path) || !mPartitionMap.get(path).getmIndirectionColumn().containsKey(key)) {
				System.out.println("Error reading the value");
				return null;
			}
			//Check for the case where value is -1;
			return mPartitionMap.get(path).getmIndirectionColumn().get(key);
		}
		return null;
	}

	@Override
	public void put(String path, ByteBuffer key, ByteBuffer value)
			throws AlluxioTException, ThriftIOException, TException {
		if(isPathValid(path)) {
			createOrReturnIndirectionColumn(path).put(key, value);
			try {
			    mPartitionMap.get(path).getMbWriter().write(key.array(), value.array());
			    mPartitionMap.get(path).getMbWriter().flush();
				long mergeOffset =  mPartitionMap.get(path).getMmergeOffset() + key.array().length  + value.array().length;
				mPartitionMap.get(path).setMmergeOffset(mergeOffset);
				System.out.println("mergeOffset: "+mPartitionMap.get(path).getMmergeOffset());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
		}
		
	}
	
	private boolean isPathValid(String path) {
		if(path != null)
				return true;
		return false;
	}
	
	
	
	private Map<ByteBuffer,ByteBuffer> createOrReturnIndirectionColumn(String path) {
		//If indirection Column exists for the given path. Then return it.
		//Else create a new indirection Column and tail page file. Add those entries and return the indirection column.
		try {
			if(!mPartitionMap.containsKey(path)) {  //Initializing the Key-value Store Variables
				initializeKVStore(path);
				System.out.println("Initializing KV Store");
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//Add the entry that this map is corresponding to the path given.
		return mPartitionMap.get(path).getmIndirectionColumn();
		
	}
	
	private void initializeKVStore(String path) throws FileNotFoundException {
	    mPartitionMap.put(path,new MuKVWorkerPartitionInfo(path));
	}

	@Override
	public void deleteKey(String path, ByteBuffer key) throws AlluxioTException, ThriftIOException, TException {
		//check if the indirectionColumn is present??? Cannot delete a key value pair before it is created.
		//Puts -1 for the value field and puts the value in the file and then the indirection Column
		if(mPartitionMap.containsKey(path) && mPartitionMap.get(path).getmIndirectionColumn().containsKey(key))
				mPartitionMap.get(path).getmIndirectionColumn().put(key,ByteBuffer.allocate(4).putInt(-1));
		else
			System.out.println("No such value present to be deleted");
		
		return;
	}

}
