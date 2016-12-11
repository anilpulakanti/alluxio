package alluxio.server.mkeyvalue;

import alluxio.thrift.AlluxioTException;
import alluxio.thrift.ThriftIOException;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.mkeyvalue.Merge;
import alluxio.worker.mkeyvalue.MuKeyValueWorkerClientServiceHandler;

import java.nio.ByteBuffer;

import org.apache.thrift.TException;

public class BaseOperationsWorkerTest {

	public BaseOperationsWorkerTest() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		
		System.out.println("Starting the testing for workers");
		
		String path = "/Dummy.txt";
		
		BlockWorker bw = null;
		
		MuKeyValueWorkerClientServiceHandler handler = new MuKeyValueWorkerClientServiceHandler(bw);
		//Merge mmergeProcess = new Merge(handler.getmPartitionMap());
		//Thread t = new Thread(mmergeProcess);
		System.out.println("Came back from the thread");
		byte[] key = null, value = null;
        try {
        key = ByteBuffer.allocate(4).putInt(1).array();
        value = ByteBuffer.allocate(4).putInt(3).array();
		handler.put(path, ByteBuffer.wrap(key), ByteBuffer.wrap(value));
		
		key = ByteBuffer.allocate(4).putInt(2).array();
		value = ByteBuffer.allocate(4).putInt(4).array();
		handler.put(path, ByteBuffer.wrap(key), ByteBuffer.wrap(value));
        }
        catch(Exception e) {
        	System.out.println("Problem with Inserting the Key value pair");
        }
        key = ByteBuffer.allocate(4).putInt(1).array();
        try {
			System.out.println("The value is: "+handler.get(path, ByteBuffer.wrap(key)).getInt(0));
			//System.out.println("The value is: "+handler.get(path, ByteBuffer.allocate(4).putInt(2)).getInt(0));
		} catch (AlluxioTException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ThriftIOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        Merge mmergeProcess = new Merge(handler.getmPartitionMap());
		Thread t = new Thread(mmergeProcess);
        t.start();
        try {
			Thread.sleep(4000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        mmergeProcess.terminate();
        try {
			t.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
