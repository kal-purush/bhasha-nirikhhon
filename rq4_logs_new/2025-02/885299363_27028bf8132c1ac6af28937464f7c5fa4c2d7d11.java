package com.yandex.kanban.servers;

import com.google.gson.*;
import com.yandex.kanban.model.Epic;
import com.yandex.kanban.model.Subtask;
import com.yandex.kanban.model.Task;

import java.lang.reflect.Type;

public class TaskDeserializer implements JsonDeserializer<Task> {

    @Override
    public Task deserialize(JsonElement jsonElement, Type typeOff, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        JsonObject jsonObject = jsonElement.getAsJsonObject();

        // Извлекаем тип задачи
        String type = jsonObject.

        // В зависимости от типа создаем соответствующий объект
        if ("SUBTASK".equals(type)) {
            return jsonDeserializationContext.deserialize(jsonObject, Subtask.class);
        } else if ("EPIC".equals(type)){
            return jsonDeserializationContext.deserialize(jsonObject, Epic.class);
        } else
            return jsonDeserializationContext.deserialize(jsonObject, Task.class);
    }
}