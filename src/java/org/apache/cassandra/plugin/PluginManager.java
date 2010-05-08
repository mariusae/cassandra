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

import java.util.List;
import java.util.ArrayList;

import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.RowMutation;

public class PluginManager implements Plugin
{
    List<Plugin> plugins = new ArrayList<Plugin>();

    /*
     * Add the given instance to the list of managed plugins. After
     * this, the plugin will receive callbacks.
     */
    public void addInstance(Plugin instance)
    {
        instance.initialize();
        plugins.add(instance);
    }

    public boolean removeInstance(Plugin instance)
    {
        return plugins.remove(instance);
    }

    /*
     * Given a string naming a class, instantiate it as a Plugin and
     * add it to the list of managed plugins.
     */
    public void addClass(String pluginClass)
    {
        try 
        {
            Class<Plugin> cls = (Class<Plugin>)Class.forName(pluginClass);
            Plugin instance = cls.getConstructor().newInstance();
            addInstance(instance);
        } 
        catch (Exception e) 
        {
            throw new RuntimeException(e);
        }
    }

    public void initialize() {}

    public Runnable beforeMutate(Table table, RowMutation mutation)
    {
        final List<Runnable> afterMutates = new ArrayList<Runnable>();

        for (Plugin plugin : plugins)
        {
            Runnable r = plugin.beforeMutate(table, mutation);
            if (r != null)
                afterMutates.add(r);
        }

        if (!afterMutates.isEmpty())
        {
            return new Runnable() {
                public void run() {
                    for (Runnable after : afterMutates)
                        after.run();
                }
            };
        }
        else
        {
            return null;
        }
    }
}
