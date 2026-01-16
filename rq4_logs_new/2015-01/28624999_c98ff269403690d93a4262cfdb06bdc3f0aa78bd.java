/**
 * Copyright (c) 2012-2015 Nord Trading Network.
 *
 * http://www.nordpos.mobi
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package mobi.nordpos.dao.model;

import com.j256.ormlite.dao.ForeignCollection;
import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.field.ForeignCollectionField;
import com.j256.ormlite.table.DatabaseTable;
import java.util.UUID;

/**
 * @author Andrey Svininykh <svininykh@gmail.com>
 */
@DatabaseTable(tableName = "TICKETS")
public final class Ticket {

    public enum TicketType {

        SELL(0);
        private final int code;

        private TicketType(int code) {
            this.code = code;
        }

        public int getTicketType() {
            return code;
        }
    }

    public static final String ID = "ID";
    public static final String TICKETTYPE = "TICKETTYPE";
    public static final String TICKETID = "TICKETID";
    public static final String PERSON = "PERSON";
    public static final String CUSTOMER = "CUSTOMER";
    public static final String STATUS = "STATUS";

    @DatabaseField(id = true, columnName = ID, canBeNull = false)
    private UUID id;

    @DatabaseField(columnName = TICKETTYPE, canBeNull = false, uniqueCombo = true, indexName = "TICKETS_TICKETID", defaultValue = "0")
    private Integer type;

    @DatabaseField(columnName = TICKETID, canBeNull = false, uniqueCombo = true, indexName = "TICKETS_TICKETID")
    private Integer number;

    @DatabaseField(foreign = true, foreignAutoRefresh = true, foreignColumnName = User.ID, columnName = PERSON, canBeNull = false)
    private User user;

    @DatabaseField(foreign = true, foreignAutoRefresh = true, foreignColumnName = Customer.ID, columnName = CUSTOMER, canBeNull = true)
    private Customer customer;

    @ForeignCollectionField
    private ForeignCollection<TicketLine> ticketLineCollection;

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public Integer getNumber() {
        return number;
    }

    public void setNumber(Integer number) {
        this.number = number;
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }

    public Customer getCustomer() {
        return customer;
    }

    public void setCustomer(Customer customer) {
        this.customer = customer;
    }

    public ForeignCollection<TicketLine> getTicketLineCollection() {
        return ticketLineCollection;
    }

}