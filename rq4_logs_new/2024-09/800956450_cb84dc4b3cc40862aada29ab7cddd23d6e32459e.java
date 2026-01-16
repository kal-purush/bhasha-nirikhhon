package io.github.xpakx.images_integration.transformation.model;

public enum Operation {
    Read, Update, Create, Delete, Undefined;

    public static Operation toOp(String op) {
        return switch (op) {
            case "u" -> Operation.Update;
            case "c" -> Operation.Create;
            case "d" -> Operation.Delete;
            case "r" -> Operation.Read;
            default -> Operation.Undefined;
        };
    }
}