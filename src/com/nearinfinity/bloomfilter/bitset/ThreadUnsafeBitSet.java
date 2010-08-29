/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nearinfinity.bloomfilter.bitset;

import java.io.Serializable;

/**
 * Normal bit set that uses long as words.
 * @author Aaron McCurry (amccurry@nearinfinity.com)
 */
public class ThreadUnsafeBitSet extends BloomFilterBitSet implements Cloneable, Serializable {
	private static final long serialVersionUID = -3501250921524429562L;

	private long[] bits;

	public ThreadUnsafeBitSet(long numBits) {
		bits = new long[bits2words(numBits)];
	}

	public ThreadUnsafeBitSet(long[] bits) {
		this.bits = bits;
	}

	@Override
	public boolean get(long index) {
		int i = (int) (index >> 6);
		int bit = (int) index & 0x3f;
		long bitmask = 1L << bit;
		return (bits[i] & bitmask) != 0;
	}

	@Override
	public void set(long index) {
		int wordNum = (int) (index >> 6);
		int bit = (int) index & 0x3f;
		long bitmask = 1L << bit;
		bits[wordNum] |= bitmask;
	}

	@Override
	public long getMemorySize() {
		return (long) bits.length * 8L;
	}
	
	public static int bits2words(long numBits) {
		return (int) (((numBits - 1) >>> 6) + 1);
	}

}
