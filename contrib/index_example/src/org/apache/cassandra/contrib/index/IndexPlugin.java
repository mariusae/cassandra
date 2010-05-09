package org.apache.cassandra.contrib.index;

import java.util.List;
import java.util.LinkedList;

import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.plugin.Plugin;

public class IndexPlugin implements Plugin
{
    public void initialize()
    {
        /* nothing to do */
    }

    public Runnable beforeMutate(Table table, RowMutation mutation)
    {
        final RowMutation savedMutation = mutation;

        for (ColumnFamily columnFamily : mutation.getColumnFamilies())
        {
            for (IColumn column : columnFamily.getSortedColumns())
            {
                if ((new String(column.name()).equals("index")))
                    return null;
            }
        }

        return new Runnable() {
            public void run()
            {
                updateIndex(savedMutation);
            }
        };
    }

    protected void updateIndex(RowMutation mutation)
    {
        StorageService ss = StorageService.instance;
        if (!ss.isInitialized())
            return;  /* not yet ready to do database ops. */

        /*
         * We only affect index mutations if it's in our local primary
         * range so that we don't replicate the index updates.
         */
        DecoratedKey dk =
            StorageService
            .getPartitioner()
            .decorateKey(mutation.key());

        if (!ss.getLocalPrimaryRange().contains(dk.token))
            return;

        /*
         * Invert the mutations.
         */
        List<RowMutation> mutations = new LinkedList<RowMutation>();
        for (ColumnFamily columnFamily : mutation.getColumnFamilies())
        {
            for (IColumn column : columnFamily.getSortedColumns())
            {
                byte[] key = column.value();
                RowMutation indexMutation = new RowMutation(mutation.getTable(), key);
                ColumnFamily cf = ColumnFamily.create(mutation.getTable(), columnFamily.name());
                cf.addColumn(new Column("index".getBytes(), mutation.key()));
                indexMutation.add(cf);
                mutations.add(indexMutation);
            }
        }

        StorageProxy.mutate(mutations);
    }
}
