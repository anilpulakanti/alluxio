package alluxio.worker.mkeyvalue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import alluxio.AlluxioURI;
import alluxio.client.keyvalue.KeyValueStoreReader;
import alluxio.client.keyvalue.KeyValueStoreWriter;
import alluxio.client.keyvalue.KeyValueSystem;
import alluxio.exception.AlluxioException;
import alluxio.worker.mkeyvalue.MuKVWorkerPartitionInfo;

public class Merge implements Runnable {
 
	private Thread t;
	Map<String,MuKVWorkerPartitionInfo> workerPartitionMap;
	MuKVWorkerPartitionInfo kvpartitionInfo;
	long prevOffset, nextOffset,offset;
	byte[] kvPair = new byte[8];
	byte[] key, value;
	RandomAccessFile raf = null;
	boolean mmergeKill = false;
	TreeMap<ByteBuffer,ByteBuffer> tmap = null;
	public Merge(Map<String,MuKVWorkerPartitionInfo> partitionMap) {
		// TODO Auto-generated constructor stub
		System.out.println("Creating Merge Thread");
		workerPartitionMap = new HashMap<>(); 
				workerPartitionMap.putAll(partitionMap);
	}
  
	public void terminate() {
		mmergeKill = true;
	}
	
	@Override
	public void run() {
		
		//Check if there is a key value store
		//If yes scan all the elements from the keyvalue store
		//Scan all the elements from the Tailpagefile and check for delete options.
		System.out.println("In the Merge Thread");
		while(!mmergeKill) {
		Iterator partition = workerPartitionMap.entrySet().iterator();
		while(partition.hasNext()) {
		Map.Entry it = (Map.Entry)partition.next();	
		kvpartitionInfo = (MuKVWorkerPartitionInfo)it.getValue();
		tmap =  kvpartitionInfo.getTmap();
		prevOffset = kvpartitionInfo.getMprevmergeOffset();
		nextOffset = kvpartitionInfo.getMmergeOffset();
		offset = prevOffset;
		
        try {
        	raf = new RandomAccessFile("/home/viswanath/Desktop/Dummy.txt", "rw");
			raf.seek(offset);
			while(offset < nextOffset) {
				raf.readFully(kvPair);
				key = Arrays.copyOfRange(kvPair, 0, 4);
				value = Arrays.copyOfRange(kvPair, 4, 8);
				//If the value is -1 remove the element from the TreeMap if it is already present.
				System.out.println("Merge Key: "+ByteBuffer.wrap(key).getInt(0) + "Merge Value: "+ByteBuffer.wrap(value).getInt(0));
				tmap.put(ByteBuffer.wrap(key), ByteBuffer.wrap(value));
				offset += 8;
			}
			raf.close();
			kvpartitionInfo.setMprevmergeOffset(nextOffset);
			} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			}
          kvpartitionInfo.setTmap(tmap);
          //write the values to the KV Store
          AlluxioURI mStoreUri = new AlluxioURI((String)it.getKey() +"Merge_KVStore1");
          KeyValueSystem kvs = KeyValueSystem.Factory.create();

          KeyValueStoreWriter writer;
		try {
			writer = kvs.createStore(mStoreUri);
			  Iterator itTree = tmap.entrySet().iterator();
	          while(itTree.hasNext()) {
	              Map.Entry itT = (Map.Entry)itTree.next();
	              key = ((ByteBuffer)itT.getKey()).array();
	              value = ((ByteBuffer)itT.getValue()).array();
	        	  writer.put(key, value);
	        	}
	          writer.close();
	          
	          KeyValueStoreReader reader = kvs.openStore(mStoreUri);
	          System.out.println("Reading from the KV Store");
	          for (Map.Entry<ByteBuffer, ByteBuffer> pair : tmap.entrySet()) {
	              ByteBuffer expectedValue = pair.getValue();
	              ByteBuffer gotValue = reader.get(pair.getKey());
	              if (!expectedValue.equals(gotValue)) {
	               System.out.println("The value returned from the key-value store iterator is unexpected");
	                }
	            System.out.println("Success !");  
	            reader.close();
	          } 
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AlluxioException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
      
          
		}
		try {
			Thread.sleep(4000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
        //Put all the elements in the TreeMap in the KV Store 
		
		
		// TODO Auto-generated method stub

  }

}
