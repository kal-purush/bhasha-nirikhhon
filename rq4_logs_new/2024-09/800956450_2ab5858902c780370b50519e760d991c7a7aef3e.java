package io.github.xpakx.images_cdc.debezium.model;

import java.util.Optional;

public enum TableName {
    Account, Image;

    public static Optional<TableName> toName(String string) {
        return switch (string) {
            case "postgres_.public.account.Key" -> Optional.of(TableName.Account);
            case "postgres_.public.image.Key" -> Optional.of(TableName.Image);
            default -> Optional.empty();
        };
    }

    public Class<?> getType() {
        return switch (this) {
            case Account -> io.github.xpakx.images_cdc.debezium.model.Account.class;
            case Image -> Object.class;
        };
    }
}