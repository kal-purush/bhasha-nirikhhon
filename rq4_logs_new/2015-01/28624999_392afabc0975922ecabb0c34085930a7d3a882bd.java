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
import com.j256.ormlite.field.DataType;
import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.field.ForeignCollectionField;
import com.j256.ormlite.table.DatabaseTable;
import java.util.Date;
import java.util.UUID;

/**
 * @author Andrey Svininykh <svininykh@gmail.com>
 */
@DatabaseTable(tableName = "RECEIPTS")
public class Receipt {

    public static final String ID = "ID";
    public static final String MONEY = "MONEY";
    public static final String DATENEW = "DATENEW";
    public static final String ATTRIBUTES = "ATTRIBUTES";

    @DatabaseField(generatedId = true, columnName = ID)
    private UUID id;

    @DatabaseField(foreign = true, foreignAutoRefresh = true, foreignColumnName = "MONEY", columnName = MONEY, canBeNull = false, index = true, indexName = "RECEIPTS_MONEY_INX")
    private ClosedCash closedCash;

    @DatabaseField(columnName = DATENEW, canBeNull = false, index = true, indexName = "RECEIPTS_INX_1")
    private Date date;

    @DatabaseField(columnName = ATTRIBUTES, dataType = DataType.SERIALIZABLE)
    byte[] attributes;

    @DatabaseField(persisted = false)
    Ticket ticket;

    @ForeignCollectionField
    ForeignCollection<Payment> paymentCollection;

    @ForeignCollectionField
    ForeignCollection<TaxLine> taxLineCollection;

    public UUID getId() {
        return id;
    }

    public ClosedCash getClosedCash() {
        return closedCash;
    }

    public void setClosedCash(ClosedCash closedCash) {
        this.closedCash = closedCash;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public byte[] getAttributes() {
        return attributes;
    }

    public void setAttributes(byte[] attributes) {
        this.attributes = attributes;
    }

    public Ticket getTicket() {
        return ticket;
    }

    public void setTicket(Ticket ticket) {
        this.ticket = ticket;
    }

    public ForeignCollection<Payment> getPaymentCollection() {
        return paymentCollection;
    }

    public ForeignCollection<TaxLine> getTaxLineCollection() {
        return taxLineCollection;
    }

}