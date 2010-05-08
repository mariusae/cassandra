package org.apache.cassandra.contrib.index;

import java.util.List;
import java.util.ArrayList;

import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.service.StorageProxy;

import org.apache.cassandra.plugin.Plugin;

public class IndexPlugin implements Plugin
{
    public void initialize()
    {
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
                else {
                    System.out.println("received col " + column.name());
                    System.out.println("received col(s) " + (new String(column.name())));
                }
            }
        }

        return new Runnable() {
            public void run()
            {
                indexMutation(savedMutation);
            }
        };
    }

    protected void indexMutation(RowMutation mutation)
    {
        List<RowMutation> mutations = new ArrayList<RowMutation>();

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
                // key -> mutation.key()
                System.out.println(" NEW INDEX!! " + (new String(key)) + " -> " + new String(mutation.key()));
            }
        }

        StorageProxy.mutate(mutations);
    }
}
