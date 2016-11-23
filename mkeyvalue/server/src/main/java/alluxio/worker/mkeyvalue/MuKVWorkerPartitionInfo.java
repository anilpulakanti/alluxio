package alluxio.worker.mkeyvalue;

import java.nio.*;
import java.util.HashMap;
import java.util.Map;

import alluxio.AlluxioURI;

public class MuKVWorkerPartitionInfo {
		
	public Map<ByteBuffer, ByteBuffer> getmIndirectionColumn() {
		return mIndirectionColumn;
	}
	public void setmIndirectionColumn(Map<ByteBuffer, ByteBuffer> mIndirectionColumn) {
		this.mIndirectionColumn = mIndirectionColumn;
	}
	public long getMmergeOffset() {
		return mmergeOffset;
	}
	public void setMmergeOffset(long mmergeOffset) {
		this.mmergeOffset = mmergeOffset;
	}
	
	public MuKVWorkerPartitionInfo() {
		// TODO Auto-generated constructor stub
		mIndirectionColumn = new HashMap<>();
		mmergeOffset = 0;
	}
	
	private Map<ByteBuffer,ByteBuffer> mIndirectionColumn = null;
    private long mmergeOffset = 0;
}
