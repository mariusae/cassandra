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

package org.apache.cassandra.plugin;

import static org.apache.cassandra.Util.column;

import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.Util;

import org.apache.cassandra.CleanupHelper;
import org.junit.Test;
 
public class SimpleTest extends CleanupHelper
{
    class SimplePlugin implements Plugin
    {
        public Boolean didInitialize = false;
        public Table table;
        public RowMutation mutation;
        public Boolean didAfterMutation = false;

        public void initialize()
        {
            didInitialize = true;
        }

        public Runnable beforeMutate(Table table, RowMutation mutation)
        {
            this.table = table;
            this.mutation = mutation;

            return new Runnable() {
                public void run()
                {
                    didAfterMutation = true;
                }
            };
        }
    }

    @Test
    public void testSimpleMutationHook() throws Exception
    {
        SimplePlugin simplePlugin = new SimplePlugin();

        try
        {
            final DecoratedKey testKey = Util.dk("key1");
            final String keyspace = "Keyspace2";

            DatabaseDescriptor.getPluginManager().addInstance(simplePlugin);
            assert simplePlugin.didInitialize;

            final Table table = Table.open(keyspace);
            RowMutation mutation = new RowMutation(keyspace, testKey.key);

            ColumnFamily cf = ColumnFamily.create(keyspace, "Standard3");
            cf.addColumn(column("col1", "val1", 1L));

            mutation.add(cf);
            mutation.apply();

            assert simplePlugin.table == table;
            assert simplePlugin.mutation == mutation;
            assert simplePlugin.didAfterMutation;
        }
        finally
        {
            DatabaseDescriptor.getPluginManager().removeInstance(simplePlugin);
        }
    }
}