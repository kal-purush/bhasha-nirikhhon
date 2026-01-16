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

package edu.wisc.my.portlets.dmp.dao.filter;

import edu.wisc.my.portlets.dmp.beans.MenuItem;
import edu.wisc.my.portlets.dmp.dao.memory.InMemoryMenuDao;
import junit.framework.TestCase;
import org.junit.Test;

/**
 * Test cases for the filtering menu DAO.
 */
public class FilteringMenuDaoTest extends TestCase {

    /**
     * Test that the filtering DAO returns null when the user is not in any of the groups that
     * can see a root menu item.
     */
    @Test
    public void testFiltersToNullWhenNotInGroupsForWholeMenu() {

        final MenuItem rootMenuItem = new MenuItem();
        rootMenuItem.setGroups(new String[] {"a_group_the_user_is_not_in"});

        final InMemoryMenuDao underlyingDao = new InMemoryMenuDao();
        underlyingDao.storeMenu("aMenu", rootMenuItem);

        final FilteringMenuDao filteringWrapper = new FilteringMenuDao();
        filteringWrapper.setDelegateMenuDao(underlyingDao);

        assertNull(filteringWrapper.getMenu("aMenu",
            new String[] {"groups", "not_including", "the_one_group", "granted_this_menu"} ));

    }

    /**
     * Test that the filtering DAO passes through a root menu granted to a group the user is in.
     */
    @Test
    public void testAuthorizedMenuPassesThroughFilter() {

        final MenuItem rootMenuItem = new MenuItem();
        rootMenuItem.setGroups(new String[] {"yes_in_this_group", "some_other_irrelevant_group"});

        final InMemoryMenuDao underlyingDao = new InMemoryMenuDao();
        underlyingDao.storeMenu("aMenu", rootMenuItem);

        final FilteringMenuDao filteringWrapper = new FilteringMenuDao();
        filteringWrapper.setDelegateMenuDao(underlyingDao);

        final MenuItem actual =  filteringWrapper.getMenu("aMenu",
            new String[] {"yes_in_this_group", "in_other_groups_too_but_they_do_not_matter"});

        // this assertion is goofy (rather than assertEquals( expected, actual) because of
        // a weirdness in the MenuItem implementation of equals() , which rabbit hole the test
        // author did not choose to immediately pursue.
        assertTrue( actual.equals(rootMenuItem));

    }

}