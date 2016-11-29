package alluxio.worker.mkeyvalue;

import java.io.FileNotFoundException;
import java.io.File;
import java.nio.*;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import alluxio.AlluxioURI;


public class MuKVWorkerPartitionInfo {
	
	private Map<ByteBuffer,ByteBuffer> mIndirectionColumn = null;
    private long mmergeOffset = 0;
    private TailPageFileOutputStream mbWriter = null;
	private long mprevmergeOffset = 0;
	private TreeMap<ByteBuffer,ByteBuffer> tmap = null;
	
	public TreeMap<ByteBuffer, ByteBuffer> getTmap() {
		return tmap;
	}
	public void setTmap(TreeMap<ByteBuffer, ByteBuffer> tmap) {
		this.tmap = tmap;
	}
	public long getMprevmergeOffset() {
		return mprevmergeOffset;
	}
	public void setMprevmergeOffset(long mprevmergeOffset) {
		this.mprevmergeOffset = mprevmergeOffset;
	}
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
	
	public TailPageFileOutputStream getMbWriter() {
		return mbWriter;
	}
	public void setMbWriter(TailPageFileOutputStream mbWriter) {
		this.mbWriter = mbWriter;
	}
	
	
	public MuKVWorkerPartitionInfo(String path) {
		// TODO Auto-generated constructor stub
		mIndirectionColumn = new HashMap<>();
		mmergeOffset = 0;
		mprevmergeOffset = 0;
		try {
			//File yourFile = new File("~/"+path);
			//yourFile.createNewFile();
			System.out.println("Created new file "+path);
			mbWriter = new TailPageFileOutputStream("~/"+path);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
}
