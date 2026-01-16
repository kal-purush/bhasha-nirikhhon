// <copyright file="ArgumentExceptionConverter.cs" company="JSSoft">
//   Copyright (c) 2024 Jeesu Choi. All Rights Reserved.
//   Licensed under the MIT License. See LICENSE.md in the project root for license information.
// </copyright>

using System;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace JSSoft.Communication.Converters;

internal sealed class ArgumentExceptionConverter : JsonConverter<ArgumentException>
{
    public const string ParamName = "paramName";
    public const string Message = "message";

    public override ArgumentException? Read(
        ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        var props = reader.ReadObject(capacity: 2);
        var message = props[Message] ?? throw new JsonException($"{Message} is not found");
        return new ArgumentException(message);
    }

    public override void Write(
        Utf8JsonWriter writer, ArgumentException value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        writer.WriteString(Message, value.Message);
        writer.WriteString(ParamName, value.ParamName);
        writer.WriteEndObject();
    }
}