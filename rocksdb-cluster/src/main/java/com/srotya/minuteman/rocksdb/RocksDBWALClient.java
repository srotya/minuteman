package com.srotya.minuteman.rocksdb;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDB;

import com.srotya.minuteman.rocksdb.rpc.KeyValuePair;
import com.srotya.minuteman.rocksdb.rpc.PutRequest;
import com.srotya.minuteman.wal.LocalWALClient;
import com.srotya.minuteman.wal.WAL;

public class RocksDBWALClient extends LocalWALClient {

	private RocksDB rocksdb;
	private FlushOptions opts;

	@SuppressWarnings("resource")
	@Override
	public LocalWALClient configure(Map<String, String> conf, String nodeId, WAL localWAL, Object storageObject)
			throws IOException {
		super.configure(conf, nodeId, localWAL, storageObject);
		rocksdb = (RocksDB) storageObject;
		opts = new FlushOptions().setWaitForFlush(false);
		return this;
	}

	@Override
	public void processData(List<byte[]> list) {
		try {
			for (byte[] entry : list) {

//				System.out.println("Inserting data:" + entry.length);
				PutRequest request = PutRequest.parseFrom(entry);
				if (request.getDelete()) {
					for (KeyValuePair keyValuePair : request.getKvPairsList()) {
						rocksdb.remove(keyValuePair.getKey().getBytes());
					}
				} else {
					for (KeyValuePair keyValuePair : request.getKvPairsList()) {
						rocksdb.put(keyValuePair.getKey().getBytes(), keyValuePair.getValue().toByteArray());
					}
				}
			}
			rocksdb.flush(opts);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
