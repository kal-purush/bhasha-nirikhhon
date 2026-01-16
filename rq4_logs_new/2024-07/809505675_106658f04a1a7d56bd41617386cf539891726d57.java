package com.fitness.pass.pass_batch.repository.notification;

import com.fitness.pass.pass_batch.repository.booking.BookingEntity;
import com.fitness.pass.pass_batch.repository.user.UserEntity;
import javax.annotation.processing.Generated;

@Generated(
    value = "org.mapstruct.ap.MappingProcessor",
    date = "2024-07-08T19:19:34+0900",
    comments = "version: 1.5.2.Final, compiler: javac, environment: Java 17.0.7 (Amazon.com Inc.)"
)
public class NotificationModelMapperImpl implements NotificationModelMapper {

    @Override
    public NotificationEntity toNotificationEntity(BookingEntity bookingEntity, NotificationEvent event) {
        if ( bookingEntity == null && event == null ) {
            return null;
        }

        NotificationEntity notificationEntity = new NotificationEntity();

        if ( bookingEntity != null ) {
            notificationEntity.setUuid( bookingEntityUserEntityUuid( bookingEntity ) );
            notificationEntity.setText( text( bookingEntity.getStartedAt() ) );
        }
        notificationEntity.setEvent( event );

        return notificationEntity;
    }

    private String bookingEntityUserEntityUuid(BookingEntity bookingEntity) {
        if ( bookingEntity == null ) {
            return null;
        }
        UserEntity userEntity = bookingEntity.getUserEntity();
        if ( userEntity == null ) {
            return null;
        }
        String uuid = userEntity.getUuid();
        if ( uuid == null ) {
            return null;
        }
        return uuid;
    }
}