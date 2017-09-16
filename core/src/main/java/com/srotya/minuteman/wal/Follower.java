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

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;

/**
 * @author ambud
 */
public class Follower {

	private int fileId;
	private int offset;
	private RandomAccessFile reader;
	private MappedByteBuffer buf; 

	/**
	 * @return the reader
	 */
	public RandomAccessFile getReader() {
		return reader;
	}

	/**
	 * @param reader the reader to set
	 */
	public void setReader(RandomAccessFile reader) {
		this.reader = reader;
	}

	/**
	 * @return the buf
	 */
	public MappedByteBuffer getBuf() {
		return buf;
	}

	/**
	 * @param buf the buf to set
	 */
	public void setBuf(MappedByteBuffer buf) {
		this.buf = buf;
	}

	/**
	 * @return the fileId
	 */
	public int getFileId() {
		return fileId;
	}

	/**
	 * @param fileId
	 *            the fileId to set
	 */
	public void setFileId(int fileId) {
		this.fileId = fileId;
	}

	/**
	 * @return the offset
	 */
	public int getOffset() {
		return offset;
	}

	/**
	 * @param offset
	 *            the offset to set
	 */
	public void setOffset(int offset) {
		this.offset = offset;
	}

}