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

import static org.agrona.BitUtil.isPowerOfTwo;

import java.nio.ByteBuffer;
import java.util.BitSet;

import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Hashing;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * A chunk of shared memory for temporary storage of data. This is logically segmented into a set of
 * slots of equal size. Methods are provided for acquiring a slot, getting a buffer that can be used
 * to store data in it, and releasing the slot once it is no longer needed.
 * <b>Each instance of this class is assumed to be used by one and only one thread.</b>
 */
public class Slab
{
    static final int SLOT_NOT_AVAILABLE = -1;

    private final MutableDirectBuffer mutableFW = new UnsafeBuffer(new byte[0]);

    private final int slotCapacity;
    private final int bitsPerSlot;
    private final int mask;
    private final MutableDirectBuffer buffer;
    private final BitSet used;

    private int availableSlots;

    public Slab(int totalCapacity, int slotCapacity)
    {
        if (!isPowerOfTwo(totalCapacity))
        {
            throw new IllegalArgumentException("totalCapacity is not a power of 2");
        }
        if (!isPowerOfTwo(slotCapacity))
        {
            throw new IllegalArgumentException("slotCapacity is not a power of 2");
        }
        if (slotCapacity > totalCapacity)
        {
            throw new IllegalArgumentException("slotCapacity exceeds totalCapacity");
        }
        this.slotCapacity = slotCapacity;
        this.bitsPerSlot = Integer.numberOfTrailingZeros(slotCapacity);
        int totalSlots = totalCapacity / slotCapacity;
        this.mask = totalSlots - 1;
        this.buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(totalCapacity));
        this.used = new BitSet(totalSlots);
        this.availableSlots = totalSlots;
    }

    /**
     * Reserves a slot for use by the given stream
     * @param streamId - Stream id
     * @return Id of the acquired slot, or SLOT_NOT_AVAILABLE if all slots are in use
     */
    public int acquire(long streamId)
    {
        if (availableSlots == 0)
        {
            return SLOT_NOT_AVAILABLE;
        }
        int slot = Hashing.hash(streamId, mask);
        while (used.get(slot))
        {
            slot = ++slot & mask;
        }
        used.set(slot);
        availableSlots--;
        return slot;
    }

    /**
     * Gets a buffer which can be used to write data into the given slot.
     * @param slot - Id of a previously acquired slot
     * @return A buffer suitable for <b>one-time use only</b>
     */
    public MutableDirectBuffer buffer(int slot)
    {
        assert used.get(slot);
        final long slotAddressOffset = buffer.addressOffset() + (slot << bitsPerSlot);
        mutableFW.wrap(slotAddressOffset, slotCapacity);
        return mutableFW;
    }

    /**
     * Releases a slot so it may be used by other streams
     * @param slot - Id of a previously acquired slot
     */
    public void release(int slot)
    {
        assert used.get(slot);
        used.clear(slot);
        availableSlots++;
    }

}

