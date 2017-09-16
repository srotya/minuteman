/**
 * Copyright 2017 Ambud Sharma
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.srotya.minuteman.wal;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.srotya.minutema.rpc.ReplicationServiceImpl;
import com.srotya.minuteman.utils.FileUtils;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;

/**
 * @author ambud
 */
public class WALTest {

	public static void main(String[] args) throws IOException, InterruptedException {
		FileUtils.delete(new File("target/wal"));
		FileUtils.delete(new File("target/walclient"));
		WAL wal = new WAL();
		wal.configure(new HashMap<>());
		Server server = ServerBuilder.forPort(43421).addService(new ReplicationServiceImpl(wal)).build();
		server.start();
		WALClient client = new WALClient();
		Map<String, String> conf = new HashMap<>();
		conf.put("wal.dir", "target/walclient");
		ManagedChannel channel = ManagedChannelBuilder.forAddress("127.0.0.1", 43421).usePlaintext(true).build();
		client.configure(conf, "l1", channel);
		Thread thread = new Thread(client);
		thread.start();
		for (int i = 0; i < 100_000_000; i++) {
			wal.write(longToBytes(System.currentTimeMillis()), false);
			if (i % 10_000_000 == 0) {
				System.out.println("Master written:" + i);
			}
		}

		while (wal.getCurrentWriteBuffer().position() != client.getPos()) {
			System.out.println("Wait..." + client.getPos() + "\t\t" + wal.getCurrentWriteBuffer().position());
			Thread.sleep(1000);
		}
		wal.getCurrentWriteBuffer().force();
		System.exit(0);
	}

	public static byte[] longToBytes(long value) {
		ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
		buffer.putLong(value);
		return buffer.array();
	}

}
