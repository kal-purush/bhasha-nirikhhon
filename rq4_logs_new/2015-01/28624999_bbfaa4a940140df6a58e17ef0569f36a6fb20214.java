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

import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.table.DatabaseTable;
import java.math.BigDecimal;
import java.util.UUID;

/**
 * @author Andrey Svininykh <svininykh@gmail.com>
 */
@DatabaseTable(tableName = "PAYMENTS")
public class Payment {

    public enum PaymentType {

        CASH("cash"), CARD("magcard"), CARD_REFUND("magcardrefund"), CHANGE("change"), UNKNOWN("unknown"), FREE("free");
        private final String type;

        private PaymentType(String type) {
            this.type = type;
        }

        public String getPaymentType() {
            return type;
        }
    }

    public static final String ID = "ID";
    public static final String RECEIPT = "RECEIPT";
    public static final String PAYMENT = "PAYMENT";
    public static final String TOTAL = "TOTAL";

    @DatabaseField(generatedId = true, columnName = ID)
    private UUID id;

    @DatabaseField(foreign = true, foreignAutoRefresh = true, columnName = RECEIPT, canBeNull = false)
    private Receipt receipt;

    @DatabaseField(columnName = PAYMENT, canBeNull = false)
    private String type;

    @DatabaseField(columnName = TOTAL, canBeNull = false)
    private BigDecimal amount;

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public Receipt getReceipt() {
        return receipt;
    }

    public void setReceipt(Receipt receipt) {
        this.receipt = receipt;
    }
}