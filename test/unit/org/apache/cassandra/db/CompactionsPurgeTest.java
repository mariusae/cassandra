/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.Util;

import static junit.framework.Assert.assertEquals;
import static org.apache.cassandra.db.TableTest.assertColumns;

public class CompactionsPurgeTest extends CleanupHelper
{
    public static final String TABLE1 = "Keyspace1";

    @Test
    public void testCompactionPurge() throws IOException, ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();

        Table table = Table.open(TABLE1);
        String cfName = "Standard1";
        ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);

        DecoratedKey key = Util.dk("key1");
        RowMutation rm;

        // inserts
        rm = new RowMutation(TABLE1, key.key);
        for (int i = 0; i < 10; i++)
        {
            rm.add(new QueryPath(cfName, null, String.valueOf(i).getBytes()), new byte[0], new TimestampClock(0));
        }
        rm.apply();
        cfs.forceBlockingFlush();

        // deletes
        for (int i = 0; i < 10; i++)
        {
            rm = new RowMutation(TABLE1, key.key);
            rm.delete(new QueryPath(cfName, null, String.valueOf(i).getBytes()), new TimestampClock(1));
            rm.apply();
        }
        cfs.forceBlockingFlush();

        // resurrect one column
        rm = new RowMutation(TABLE1, key.key);
        rm.add(new QueryPath(cfName, null, String.valueOf(5).getBytes()), new byte[0], new TimestampClock(2));
        rm.apply();
        cfs.forceBlockingFlush();

        // verify that non-major compaction does no GC to ensure correctness (see CASSANDRA-604)
        Collection<SSTableReader> sstablesIncomplete = cfs.getSSTables();
        rm = new RowMutation(TABLE1, Util.dk("blah").key);
        rm.add(new QueryPath(cfName, null, "0".getBytes()), new byte[0], new TimestampClock(0));
        rm.apply();
        cfs.forceBlockingFlush();
        CompactionManager.instance.doCompaction(cfs, sstablesIncomplete, (int) (System.currentTimeMillis() / 1000) - cfs.metadata.gcGraceSeconds);
        ColumnFamily cf = cfs.getColumnFamily(QueryFilter.getIdentityFilter(key, new QueryPath(cfName)));
        assert cf.getColumnCount() == 10;

        // major compact and test that all columns but the resurrected one is completely gone
        CompactionManager.instance.submitMajor(cfs, 0, Integer.MAX_VALUE).get();
        cfs.invalidateCachedRow(key);
        cf = cfs.getColumnFamily(QueryFilter.getIdentityFilter(key, new QueryPath(cfName)));
        assertColumns(cf, "5");
        assert cf.getColumn(String.valueOf(5).getBytes()) != null;
    }

    @Test
    public void testCompactionPurgeOneFile() throws IOException, ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();

        Table table = Table.open(TABLE1);
        String cfName = "Standard2";
        ColumnFamilyStore store = table.getColumnFamilyStore(cfName);

        DecoratedKey key = Util.dk("key1");
        RowMutation rm;

        // inserts
        rm = new RowMutation(TABLE1, key.key);
        for (int i = 0; i < 5; i++)
        {
            rm.add(new QueryPath(cfName, null, String.valueOf(i).getBytes()), new byte[0], new TimestampClock(0));
        }
        rm.apply();

        // deletes
        for (int i = 0; i < 5; i++)
        {
            rm = new RowMutation(TABLE1, key.key);
            rm.delete(new QueryPath(cfName, null, String.valueOf(i).getBytes()), new TimestampClock(1));
            rm.apply();
        }
        store.forceBlockingFlush();

        assert store.getSSTables().size() == 1 : store.getSSTables(); // inserts & deletes were in the same memtable -> only deletes in sstable

        // compact and test that the row is completely gone
        CompactionManager.instance.submitMajor(store, 0, Integer.MAX_VALUE).get();
        assert store.getSSTables().isEmpty();
        ColumnFamily cf = table.getColumnFamilyStore(cfName).getColumnFamily(QueryFilter.getIdentityFilter(key, new QueryPath(cfName)));
        assert cf == null : cf;
    }

    @Test
    public void testKeyCache50() throws IOException, ExecutionException, InterruptedException
    {
        testKeyCache("Standard3", 64);
    }

    @Test
    public void testKeyCache100() throws IOException, ExecutionException, InterruptedException
    {
        testKeyCache("Standard4", 128);
    }

    public void testKeyCache(String cfname, int expectedCacheSize) throws IOException, ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();

        Table table = Table.open(TABLE1);
        String cfName = cfname;
        ColumnFamilyStore store = table.getColumnFamilyStore(cfName);

        // KeyCache should start at size 1 if we're caching X% of zero data.
        int keyCacheSize = store.getKeyCacheSize();
        assert keyCacheSize == 1 : keyCacheSize;

        DecoratedKey key1 = Util.dk("key1");
        DecoratedKey key2 = Util.dk("key2");
        RowMutation rm;

        // inserts
        rm = new RowMutation(TABLE1, key1.key);
        rm.add(new QueryPath(cfName, null, "1".getBytes()), new byte[0], new TimestampClock(0));
        rm.apply();
        rm = new RowMutation(TABLE1, key2.key);
        rm.add(new QueryPath(cfName, null, "2".getBytes()), new byte[0], new TimestampClock(0));
        rm.apply();

        // deletes
        rm = new RowMutation(TABLE1, key1.key);
        rm.delete(new QueryPath(cfName, null, "1".getBytes()), new TimestampClock(1));
        rm.apply();
        rm = new RowMutation(TABLE1, key2.key);
        rm.delete(new QueryPath(cfName, null, "2".getBytes()), new TimestampClock(1));
        rm.apply();

        // After a flush, the cache should expand to be X% of indices * INDEX_INTERVAL.
        store.forceBlockingFlush();
        keyCacheSize = store.getKeyCacheSize();
        assert keyCacheSize == expectedCacheSize : keyCacheSize;

        // After a compaction, the cache should expand to be X% of zero data.
        CompactionManager.instance.submitMajor(store, 0, Integer.MAX_VALUE).get();
        keyCacheSize = store.getKeyCacheSize();
        assert keyCacheSize == 1 : keyCacheSize;
    }
}
