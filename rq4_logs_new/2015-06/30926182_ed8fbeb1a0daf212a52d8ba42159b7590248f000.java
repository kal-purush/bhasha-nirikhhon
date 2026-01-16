/*
 * Licensed to Apereo under one or more contributor license
 * agreements. See the NOTICE file distributed with this work
 * for additional information regarding copyright ownership.
 * Apereo licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain a
 * copy of the License at the following location:
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package edu.wisc.my.portlets.dmp.dao.memory;

import edu.wisc.my.portlets.dmp.beans.MenuItem;
import edu.wisc.my.portlets.dmp.dao.MenuDao;
//import edu.wisc.my.portlets.dmp.dao.filter.FilteringMenuItem;

import java.util.HashMap;
import java.util.Map;

/**
 * An in-memory implementation of MenuDao.
 *
 * @since 1.1
 */
public final class InMemoryMenuDao
    implements MenuDao {

    private Map<String, MenuItem> menus = new HashMap<String, MenuItem>();

    @Override
    public String[] getPublishedMenuNames() {
        return this.menus.keySet().toArray(new String[0]);
    }

    @Override
    public MenuItem getMenu(final String menuName) {
        return this.menus.get(menuName);
    }

    @Override
    public MenuItem getMenu(final String menuName, final String[] userGroups) {
        throw new UnsupportedOperationException(
            "Getting menu filtered by groups not yet supported");
        //return new FilteringMenuItem(this.menus.get(menuName), userGroups);
    }

    @Override
    public void storeMenu(final String menuName, final MenuItem rootItem) {
        this.menus.put(menuName, rootItem);
    }

    @Override
    public void deleteMenu(final String menuName) {
        this.menus.remove(menuName);
    }
}