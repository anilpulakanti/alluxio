package alluxio.server.mkeyvalue;

import alluxio.thrift.AlluxioTException;
import alluxio.thrift.ThriftIOException;
import alluxio.worker.mkeyvalue.MuKeyValueWorkerClientServiceHandler;

import java.nio.ByteBuffer;

import org.apache.thrift.TException;

public class BaseOperationsWorkerTest {

	public BaseOperationsWorkerTest() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		
		System.out.println("Starting the testing for workers");
		
		String path = "/alluxio/Dummy";
		
		MuKeyValueWorkerClientServiceHandler handler = new MuKeyValueWorkerClientServiceHandler();
        try {
        	
		handler.put(path, ByteBuffer.allocate(4).putInt(1), ByteBuffer.allocate(4).putInt(3));
        }
        catch(Exception e) {
        	System.out.println("Problem with Inserting the Key value pair");
        }
        
        try {
			System.out.println("The value is: "+handler.get(path, ByteBuffer.allocate(4).putInt(1)).getInt(0));
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
	}

}
