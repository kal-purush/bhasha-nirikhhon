package br.com.ead.sales.entities;

import br.com.ead.sales.model.OrderStatus;
import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.DateFormat;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.PastOrPresent;
import java.time.OffsetDateTime;

@Data
@With
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(indexName = "orders", versionType = Document.VersionType.EXTERNAL)
public class Order {

    @Id
    String orderId;

    @NotNull
    @Min(0)
    @Field(type = FieldType.Integer)
    Integer placeOrderAmount;

    @NotNull
    @PastOrPresent
    @Field(type = FieldType.Date, format = DateFormat.basic_date_time)
    OffsetDateTime createdAt;
}