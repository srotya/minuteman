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
package com.srotya.minuteman.rocksdb;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import com.srotya.minuteman.cluster.WALManager;
import com.srotya.minuteman.cluster.WALManagerImpl;
import com.srotya.minuteman.connectors.AtomixConnector;
import com.srotya.minuteman.connectors.ClusterConnector;
import com.srotya.minuteman.rocksdb.api.RockApi;

import io.dropwizard.Application;
import io.dropwizard.setup.Environment;

/**
 */
public class RocksDBCluster extends Application<RocksDBConfiguration> {

	@Override
	public void run(RocksDBConfiguration configuration, Environment environment) throws Exception {
		Map<String, String> conf = new HashMap<>();
		if (configuration.getConfigPath() != null) {
			loadConfiguration(configuration, conf);
		}
		int shards = Integer.parseInt(conf.getOrDefault("shard.count", "1"));

		ClusterConnector connector;
		try {
			connector = new AtomixConnector();
			connector.init(conf);
		} catch (Exception e) {
			throw new IOException(e);
		}

		RocksDB rocks = buildLocalRocksDB(conf);
		ScheduledExecutorService es = Executors.newScheduledThreadPool(1);

		conf.put(WALManager.WAL_CLIENT_CLASS, RocksDBWALClient.class.getName());
		WALManager mgr = new WALManagerImpl();
		mgr.init(conf, connector, es, rocks);

		environment.jersey().register(new RockApi(shards, mgr, rocks));
	}

	private RocksDB buildLocalRocksDB(Map<String, String> conf) throws RocksDBException {
		Options options = new Options();
		options.setAllowMmapReads(true).setAllowMmapWrites(true).setCreateIfMissing(true)
				.setCompressionType(CompressionType.SNAPPY_COMPRESSION).setCreateMissingColumnFamilies(true);
		RocksDB db = RocksDB.open(options, conf.getOrDefault("rocksdb.dir", "target/rocksdb"));
		return db;
	}

	private void loadConfiguration(RocksDBConfiguration configuration, Map<String, String> conf)
			throws IOException, FileNotFoundException {
		String path = configuration.getConfigPath();
		if (path != null) {
			Properties props = new Properties();
			props.load(new FileInputStream(path));
			for (final String name : props.stringPropertyNames()) {
				conf.put(name, props.getProperty(name));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		new RocksDBCluster().run(args);
	}

}
