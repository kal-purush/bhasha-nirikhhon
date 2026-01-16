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
import java.util.Date;
import java.util.UUID;

/**
 * @author Andrey Svininykh <svininykh@gmail.com>
 */
@DatabaseTable(tableName = "CLOSEDCASH")
public class ClosedCash {

    public static final String MONEY = "MONEY";
    public static final String HOST = "HOST";
    public static final String HOSTSEQUENCE = "HOSTSEQUENCE";
    public static final String DATESTART = "DATESTART";
    public static final String DATEEND = "DATEEND";

    @DatabaseField(generatedId = true, columnName = MONEY)
    private UUID id;

    @DatabaseField(columnName = HOST, canBeNull = false, width = 1024, uniqueCombo = true, uniqueIndexName = "CLOSEDCASH_INX_SEQ")
    private String host;

    @DatabaseField(columnName = HOSTSEQUENCE, canBeNull = false, uniqueCombo = true, uniqueIndexName = "CLOSEDCASH_INX_SEQ")
    private Integer hostSequence;

    @DatabaseField(columnName = DATESTART, canBeNull = false, unique = true, uniqueIndexName = "CLOSEDCASH_INX_1")
    private Date dateStart;

    @DatabaseField(columnName = DATEEND)
    private Date dateEnd;

    @ForeignCollectionField
    private ForeignCollection<Receipt> receiptCollection;

    public UUID getId() {
        return id;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getHostSequence() {
        return hostSequence;
    }

    public void setHostSequence(Integer hostSequence) {
        this.hostSequence = hostSequence;
    }

    public Date getDateStart() {
        return dateStart;
    }

    public void setDateStart(Date dateStart) {
        this.dateStart = dateStart;
    }

    public Date getDateEnd() {
        return dateEnd;
    }

    public void setDateEnd(Date dateEnd) {
        this.dateEnd = dateEnd;
    }
    
    public ForeignCollection<Receipt> getReceiptCollection() {
        return receiptCollection;
    }

    @Override
    public int hashCode() {
        return host.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == null || other.getClass() != getClass()) {
            return false;
        }
        return host.equals(((ClosedCash) other).host) && host.equals(((ClosedCash) other).host);
    }
}