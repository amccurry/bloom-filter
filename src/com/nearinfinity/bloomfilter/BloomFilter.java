/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nearinfinity.bloomfilter;

import java.io.Serializable;

import com.nearinfinity.bloomfilter.bitset.BloomFilterBitSet;
import com.nearinfinity.bloomfilter.bitset.ThreadSafeBitSet;
import com.nearinfinity.bloomfilter.bitset.ThreadUnsafeBitSet;


/**
 * This is a simple implementation of a bloom filter, it uses a chain of murmur
 * hashes to create the bloom filter.
 * 
 * @author amccurry@nearinfinity.com
 */
public class BloomFilter<T> extends BloomFilterFormulas implements Serializable {
	
	private static final long serialVersionUID = -4837894658242080928L;
	private static final int seed = 1;
	
	public interface ToBytes<T> extends Serializable {
		
		/**
		 * Turn the given keys into bytes.
		 * @param key the key.
		 * @return bytes that represent the key.
		 */
		byte[] toBytes(T key);
	}
	
	private BloomFilterBitSet bitSet;
	private ToBytes<T> toBytes;
	private long numberOfBitsDivBy2;
	private int hashes;

	/**
	 * Creates a bloom filter with the provided number of hashed and hits.
	 * @param hashes the hashes to be performed.
	 * @param numberOfBits the numberOfBits to be used in the bit set.
	 * @param threadSafe indicates whether the underlying bit set has to be thread safe or not.
	 */
	public BloomFilter(int hashes, long numberOfBits, ToBytes<T> toBytes, boolean threadSafe) {
		this.hashes = hashes;
		this.numberOfBitsDivBy2 = numberOfBits / 2;
		if (threadSafe) {
			this.bitSet = new ThreadSafeBitSet(numberOfBits);
		} else {
			this.bitSet = new ThreadUnsafeBitSet(numberOfBits);
		}
		this.toBytes = toBytes;
	}
	
	/**
	 * Creates a bloom filter with the provided number of hashed and hits.
	 * @param probabilityOfFalsePositives  the probability of false positives for the given number of elements.
	 * @param numberOfBits the numberOfBits to be used in the bit set.
	 * @param threadSafe indicates whether the underlying bit set has to be thread safe or not.
	 */
	public BloomFilter(double probabilityOfFalsePositives, long elementSize, ToBytes<T> toBytes, boolean threadSafe) {
		this(getOptimalNumberOfHashesByBits(elementSize, getNumberOfBits(probabilityOfFalsePositives, elementSize)),
				getNumberOfBits(probabilityOfFalsePositives, elementSize),
				toBytes, threadSafe);
	}
	
	/**
	 * Creates a thread safe bloom filter with the provided number of hashed and hits.
	 * @param hashes the hashes to be performed.
	 * @param numberOfBits the numberOfBits to be used in the bit set.
	 */
	public BloomFilter(int hashes, long numberOfBits, ToBytes<T> toBytes) {
		this(hashes,numberOfBits,toBytes,true);
	}
	
	/**
	 * Creates a thread safe bloom filter with the provided number of hashed and hits.
	 * @param probabilityOfFalsePositives  the probability of false positives for the given number of elements.
	 * @param numberOfBits the numberOfBits to be used in the bit set.
	 */
	public BloomFilter(double probabilityOfFalsePositives, long elementSize, ToBytes<T> toBytes) {
		this(probabilityOfFalsePositives,elementSize,toBytes,true);
	}
	
	/**
	 * Add a key to the bloom filter.
	 * @param key the key.
	 */
	public void add(T key) {
		byte[] bs = toBytes.toBytes(key);
		addInternal(bs);
	}

	/**
	 * Tests a key in the bloom filter, it may provide false positives.
	 * @param key the key.
	 * @return boolean.
	 */
	public boolean test(T key) {
		byte[] bs = toBytes.toBytes(key);
		return testInternal(bs);
	}
	
	/**
	 * Add a key to the bloom filter.
	 * @param key the key.
	 */
	public void addBytes(byte[] key, int offset, int length) {
		byte[] bs = key;
		for (int i = 0; i < hashes; i++) {
			int hash = MurmurHash.hash(seed, bs, offset, length);
			setBitSet(hash);
			bs[0]++;
		}
	}

	/**
	 * Tests a key in the bloom filter, it may provide false positives.
	 * @param key the key.
	 * @return boolean.
	 */
	public boolean testBytes(byte[] key, int offset, int length) {
		byte[] bs = key;
		for (int i = 0; i < hashes; i++) {
			int hash = MurmurHash.hash(seed, bs, offset, length);
			if (!testBitSet(hash)) {
				return false;
			}
			bs[0]++;
		}
		return true;
	}
	
	/**
	 * Gets the number of long words in the bit set.
	 * @return the number of bytes in the heap (not counting jvm overhead).
	 */
	public long getMemorySize() {
		return bitSet.getMemorySize();
	}

	/**
	 * Test the key against the bit set with the proper number of hashes.
	 * @param key the key.
	 * @return boolean.
	 */
	private boolean testInternal(byte[] key) {
		byte[] bs = key;
		for (int i = 0; i < hashes; i++) {
			int hash = MurmurHash.hash(seed, bs, 0, bs.length);
			if (!testBitSet(hash)) {
				return false;
			}
			bs[0]++;
		}
		return true;
	}
	
	/**
	 * Adds the key to the bit set with the proper number of hashes.
	 * @param key the key.
	 */
	private void addInternal(byte[] key) {
		byte[] bs = key;
		for (int i = 0; i < hashes; i++) {
			int hash = MurmurHash.hash(seed, bs, 0, bs.length);
			setBitSet(hash);
			bs[0]++;
		}
	}

	/**
	 * Sets the bit position in the bit set.
	 * @param hash the hash produced by the murmur class.
	 */
	private void setBitSet(int hash) {
		bitSet.set(getIndex(hash));
	}
	
	/**
	 * Tests the bit position in the bit set.
	 * @param hash the hash produced by the murmur class.
	 * @return boolean.
	 */
	private boolean testBitSet(int hash) {
		return bitSet.get(getIndex(hash));
	}
	
	/**
	 * Gets the index into the bit set for the given hash.
	 * @param hash the hash produced by the murmur class.
	 * @return the index position.
	 */
	private long getIndex(int hash) {
		return (hash % numberOfBitsDivBy2) + numberOfBitsDivBy2;
	}

}
