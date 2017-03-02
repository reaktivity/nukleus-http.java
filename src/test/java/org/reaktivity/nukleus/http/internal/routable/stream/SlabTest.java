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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.agrona.ExpandableArrayBuffer;
import org.junit.Test;

public class SlabTest
{
    private ExpandableArrayBuffer data = new ExpandableArrayBuffer(100);

    @Test
    public void acquireShouldAllocateEmptySlot() throws Exception
    {
        Slab slab = new Slab(10, 120);
        int slot = slab.acquire(123);
        assertEquals(0, slab.limit(slot) - slab.offset(slot));
    }


    @Test
    public void acquireShouldAllocateDifferentSlotsForDifferentStreams() throws Exception
    {
        Slab slab = new Slab(10, 120);
        int slot1 = slab.acquire(111);
        assertTrue(slot1 >= 0);
        int slot2 = slab.acquire(112);
        assertTrue(slot2 >= 0);

        assertNotEquals(slot1, slot2);
    }

    @Test
    public void acquireShouldAllocateDifferentSlotsForDifferentStreamsWithSameHashcode() throws Exception
    {
        Slab slab = new Slab(10, 120);

        int slot1 = slab.acquire(1);
        assertTrue(slot1 >= 0);

        int slot2 = slab.acquire(16);
        assertTrue(slot2 >= 0);

        assertNotEquals(slot1, slot2);
    }

    @Test
    public void acquireShouldReportOutOfMemory() throws Exception
    {
        Slab slab = new Slab(10, 120);
        int slot = 0;
        int i;
        for (i=0; i < findNextPositivePowerOfTwo(10); i++)
        {
            int streamId = 111 + i;
            slot = slab.acquire(streamId);
            assertTrue(slot >= 0);
        }
        slot = slab.acquire(111 + i);
        assertEquals(Slab.OUT_OF_MEMORY, slot);
    }

    @Test
    public void appendShouldStoreData() throws Exception
    {
        data.putStringWithoutLengthUtf8(13, "test data");
        Slab slab = new Slab(1, 120);
        int slot = slab.acquire(111);
        int result = slab.append(slot, data, 13, 13 + "test data".length());
        assertEquals(slot, result);
        int dataLength = slab.limit(slot) - slab.offset(slot);
        assertEquals("test data".length(), dataLength);
        assertEquals("test data", slab.buffer(slot).getStringWithoutLengthUtf8(
                slab.offset(slot), dataLength));
    }

    @Test
    public void appendShouldAppendToAlreadyStoredData() throws Exception
    {
        data.putStringWithoutLengthUtf8(0, "test data");
        Slab slab = new Slab(1, 120);
        int slot = slab.acquire(111);
        int result = slab.append(slot, data, 0, "test data".length());
        data.putStringWithoutLengthUtf8(0, " and more data");
        result = slab.append(slot, data, 0, " and more data".length());
        assertEquals(slot, result);
        int dataLength = slab.limit(slot) - slab.offset(slot);
        assertEquals("test data and more data".length(), dataLength);
        assertEquals("test data and more data", slab.buffer(slot).getStringWithoutLengthUtf8(
                slab.offset(slot), dataLength));
    }

    @Test
    public void appendShouldReportOutOfMemory() throws Exception
    {
        data.putStringWithoutLengthUtf8(0, "test data");
        Slab slab = new Slab(1, 16);
        int slot = slab.acquire(111);
        int result = slab.append(slot, data, 0, "test data".length());
        assertEquals(slot, result);
        data.putStringWithoutLengthUtf8(0, " and more data");
        result = slab.append(slot, data, 0, " and more data".length());
        assertEquals(Slab.OUT_OF_MEMORY, result);

        // verify slot has been freed
        assertEquals(-1, slab.offset(slot));
    }

    @Test
    public void freeShouldMakeSlotAvailableForReuse() throws Exception
    {
        Slab slab = new Slab(10, 120);
        int slot = 0;
        int i;
        for (i=0; i < findNextPositivePowerOfTwo(10); i++)
        {
            int streamId = 111 + i;
            slot = slab.acquire(streamId);
            assertTrue(slot >= 0);
        }
        int slotBad = slab.acquire(111 + i);
        assertEquals(Slab.OUT_OF_MEMORY, slotBad);
        slab.free(slot);
        slot = slab.acquire(111 + i);
        assertNotEquals(Slab.OUT_OF_MEMORY, slot);
    }

}

