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

import alluxio.thrift.AlluxioTException;
import alluxio.thrift.MuKeyValueWorkerClientService;
import alluxio.thrift.ThriftIOException;
import java.util.*;

/**
 * @author viswanath
 *
 */
public final class MuKeyValueWorkerClientServiceHandler implements MuKeyValueWorkerClientService.Iface  {

	@Override
	public long getServiceVersion() throws TException {
		// TODO Auto-generated method stub
		return 0;
	}

	//Contains the map from the path of the KV store to the Indirection Column
	//and the current Merge Offset
	private Map<String,MuKVWorkerPartitionInfo> mPartitionMap = new HashMap<>(); 
	TailPageFileOutputStream mBufferedWriter;
																						
	
	@Override
	public ByteBuffer get(String path, ByteBuffer key) throws AlluxioTException, ThriftIOException, TException {
		System.out.println("In get function");
		if(isPathValid(path)) {
		 
			if(!mPartitionMap.containsKey(path) || !mPartitionMap.get(path).getmIndirectionColumn().containsKey(key)) {
				System.out.println("Error reading the value");
				return null;
			}
			return mPartitionMap.get(path).getmIndirectionColumn().get(key);
		}
		return null;
	}

	@Override
	public void put(String path, ByteBuffer key, ByteBuffer value)
			throws AlluxioTException, ThriftIOException, TException {
		if(isPathValid(path)) {
			//try {
				//mBufferedWriter.write(key.array(), value.array());
			    //mPartitionMap.get(path).getmIndirectionColumn().put(key, value);
				//mPartitionMap.get(path).setMmergeOffset(mPartitionMap.get(path).getMmergeOffset()+ key.array().length + value.array().length);
			//} catch (IOException e) {
				// TODO Auto-generated catch block
			//	e.printStackTrace();
			//}
			Map<ByteBuffer,ByteBuffer> IC = createOrReturnIndirectionColumn(path);
			IC.put(key, value);
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
			if(!mPartitionMap.containsKey(path))  //Initializing the Key-value Store Variables
				initializeKVStore(path);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//Add the entry that this map is corresponding to the path given.
		return mPartitionMap.get(path).getmIndirectionColumn();
		
	}
	
	private void initializeKVStore(String path) throws FileNotFoundException {
		//mBufferedWriter = new TailPageFileOutputStream(path);
	    mPartitionMap.put(path,new MuKVWorkerPartitionInfo());
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
