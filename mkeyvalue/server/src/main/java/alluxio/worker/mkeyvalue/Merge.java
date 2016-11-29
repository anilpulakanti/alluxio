package alluxio.worker.mkeyvalue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
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
		workerPartitionMap = partitionMap;
	}

	@Override
	public void run() {
		
		//Check if there is a key value store
		//If yes scan all the elements from the keyvalue store
		//Scan all the elements from the Tailpagefile and check for delete options.
		while(!mmergeKill) {
		Iterator partition = workerPartitionMap.keySet().iterator();
		while(partition.hasNext()) {
		Map.Entry it = (Map.Entry)partition.next();	
		kvpartitionInfo = (MuKVWorkerPartitionInfo)it.getValue();
		tmap =  kvpartitionInfo.getTmap();
		prevOffset = kvpartitionInfo.getMprevmergeOffset();
		nextOffset = kvpartitionInfo.getMmergeOffset();
		offset = prevOffset;
		
        try {
        	raf = new RandomAccessFile("/home/viswanath/Desktop/test.out", "rw");
			raf.seek(offset);
			while(offset <= nextOffset) {
				raf.readFully(kvPair);
				key = Arrays.copyOfRange(kvPair, 0, 4);
				value = Arrays.copyOfRange(kvPair, 4, 8);
				//If the value is -1 remove the element from the TreeMap if it is already present.
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
		}
	}
        //Put all the elements in the TreeMap in the KV Store 
		
		
		// TODO Auto-generated method stub

  }

}
