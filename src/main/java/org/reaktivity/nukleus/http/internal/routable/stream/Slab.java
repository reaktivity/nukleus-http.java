/**
 * Copyright 2016-2017 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.reaktivity.nukleus.http.internal.routable.stream;

import static org.agrona.BitUtil.findNextPositivePowerOfTwo;

import java.util.Arrays;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Hashing;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * A chunk of shared memory for storing incomplete data for decoding purposes.
 * Methods are provided for getting a buffer and appending partial data in the case of an
 * incomplete block of data to be decoded. The memory is segmented into slots, the size of
 * each slot being the maximum allowed size for the header portion of an HTTP request or response.
 * <b>Each instance of this class is assumed to be used by one and only one thread.</b>
 */
class Slab
{
    static final int OUT_OF_MEMORY = -2;
    private static final int UNUSED = -1;
    private final int slotSize;
    private final int slotSizeBits;
    private final MutableDirectBuffer buffer;
    private final int offsets[]; // starting position for slot data in buffer
    private final int remaining[]; // length of data in slot
    private int acquiredSlots = 0;

    Slab(int minimumNumberOfSlots, int minimumSlotSize)
    {
        int slots = findNextPositivePowerOfTwo(minimumNumberOfSlots);
        slotSize = findNextPositivePowerOfTwo(minimumSlotSize);
        slotSizeBits = Integer.numberOfTrailingZeros(slotSize);
        int capacityInBytes = slots << slotSizeBits;
        buffer = new UnsafeBuffer(new byte[capacityInBytes]);
        offsets = new int[slots];
        Arrays.fill(offsets, UNUSED);
        remaining = new int[slots];
    }

    public DirectBuffer buffer(int slot)
    {
        return buffer;
    }

    public int offset(int slot)
    {
        return offsets[slot];
    }

    public int limit(int slot)
    {
        return offsets[slot] + remaining[slot];
    }

    public int append(int slot, DirectBuffer written, int offset, int limit)
    {
        assert slot >= 0 && slot < offsets.length;
        assert offsets[slot] != UNUSED;
        assert limit > offset;
        int dataLength = limit - offset;
        if (dataLength + remaining[slot] >= slotSize)
        {
            free(slot);
            return OUT_OF_MEMORY;
        }
        buffer.putBytes(offsets[slot] + remaining[slot], written, offset, dataLength);
        remaining[slot] += dataLength;
        return slot;
    }

    public int acquire(long streamId)
    {
        if (acquiredSlots == offsets.length)
        {
            return OUT_OF_MEMORY;
        }
        final int mask = offsets.length - 1;
        int slot = Hashing.hash(streamId, mask);
        while (offsets[slot] != UNUSED)
        {
            slot = ++slot & mask;
        }
        offsets[slot] = slot << slotSizeBits;
        acquiredSlots++;
        return slot;
    }

    public void free(int slot)
    {
        offsets[slot] = UNUSED;
        remaining[slot] = 0;
        acquiredSlots--;
    }

}
