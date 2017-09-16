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

/**
 * @author ambud
 */
public class WALRead {

	private int nextOffset;
	private byte[] data;
	private int fileId;

	/**
	 * @return the nextOffset
	 */
	public int getNextOffset() {
		return nextOffset;
	}

	public void setFileId(int fileCounter) {
		fileId = fileCounter;
	}

	/**
	 * @param nextOffset
	 *            the nextOffset to set
	 */
	public WALRead setNextOffset(int nextOffset) {
		this.nextOffset = nextOffset;
		return this;
	}

	/**
	 * @return the data
	 */
	public byte[] getData() {
		return data;
	}

	/**
	 * @param data
	 *            the data to set
	 */
	public WALRead setData(byte[] data) {
		this.data = data;
		return this;
	}

	public int getFileId() {
		return fileId;
	}

}