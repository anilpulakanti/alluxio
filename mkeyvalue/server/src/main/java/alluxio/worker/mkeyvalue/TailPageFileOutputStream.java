package alluxio.worker.mkeyvalue;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;


public class TailPageFileOutputStream extends FileOutputStream{

	private long mergeOffset;
	
	public long getByteOffset() {
		return mergeOffset;
	}

	public void setByteOffset(long byteOffset) {
		this.mergeOffset = byteOffset;
	}

	public TailPageFileOutputStream(String arg0) throws FileNotFoundException {
	    super(arg0);
	    mergeOffset = 0;
	}
 
    //Write a key value pair in the tail page file
	//Viswanath: The write at a particular offset is not implemented yet !!!!!!!
	
	public void write(byte[] key,byte[] value) throws IOException {
		super.write(key); super.write(value);
		mergeOffset += (key.length + value.length);
	}
	
	
}

