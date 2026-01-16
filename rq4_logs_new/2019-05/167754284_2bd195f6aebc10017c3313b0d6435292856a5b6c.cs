// ----------------------------------------------------------------------
// <copyright file="JsonActionConverter.cs" company="SoloX Software">
// Copyright (c) SoloX Software. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
// ----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Newtonsoft.Json;
using SoloX.ActionDispatch.Core.State;
using SoloX.ExpressionTools.Parser;
using SoloX.ExpressionTools.Parser.Impl;

namespace SoloX.ActionDispatch.Core.Action.Impl
{
    /// <summary>
    /// Json converter that handles the Action Json serialization.
    /// </summary>
    public class JsonActionConverter : JsonConverter
    {
        private const string IsAsynchronous = "isAsynchronous";
        private const string BehaviorType = "behaviorType";
        private const string Behavior = "behavior";
        private const string Selector = "selector";

        private static readonly Type GenericAsyncActionType = typeof(AsyncAction<,>);
        private static readonly Type GenericSyncActionType = typeof(SyncAction<,>);
        private static readonly Type GenericActionBehaviorAsyncType = typeof(IActionBehaviorAsync<,>);
        private static readonly Type GenericActionBehaviorType = typeof(IActionBehavior<,>);

        /// <inheritdoc/>
        public override bool CanConvert(Type objectType)
        {
            if (!objectType.IsGenericType)
            {
                return false;
            }

            var genericType = objectType.GetGenericTypeDefinition();
            return objectType.IsGenericType
                && (ReferenceEquals(genericType, GenericAsyncActionType)
                || ReferenceEquals(genericType, GenericSyncActionType));
        }

        /// <inheritdoc/>
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            var valueType = value.GetType();

            // Check value type
            if (!valueType.IsGenericType)
            {
                throw new ArgumentException("unsupported value type");
            }

            var genericValueType = valueType.GetGenericTypeDefinition();

            if (!ReferenceEquals(genericValueType, GenericAsyncActionType)
                && !ReferenceEquals(genericValueType, GenericSyncActionType))
            {
                throw new ArgumentException("unsupported value type");
            }

            dynamic action = value;

            writer.WriteStartObject();

            var isAsync = ReferenceEquals(genericValueType, GenericAsyncActionType);

            writer.WritePropertyName(IsAsynchronous);
            writer.WriteValue(isAsync);

            writer.WritePropertyName(BehaviorType);
            var behaviorType = action.Behavior.GetType();
            serializer.Serialize(writer, $"{behaviorType.FullName}, {behaviorType.Assembly.FullName}");

            writer.WritePropertyName(Behavior);
            serializer.Serialize(writer, action.Behavior);

            writer.WritePropertyName(Selector);
            serializer.Serialize(writer, action.Selector.ToString());

            writer.WriteEndObject();
        }

        /// <inheritdoc/>
        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            var done = false;
            bool? isAsync = null;
            string selector = null;
            Type behaviorType = null;
            dynamic behavior = null;

            var tknType = reader.TokenType;
            while (!done)
            {
                switch (tknType)
                {
                    case JsonToken.StartObject:
                        tknType = ReadNextToken(reader);
                        break;
                    case JsonToken.PropertyName:
                        var name = reader.Path;
                        switch (name)
                        {
                            case IsAsynchronous:
                                isAsync = ReadAsBoolean(reader);
                                break;
                            case Selector:
                                selector = ReadAsString(reader);
                                break;
                            case BehaviorType:
                                behaviorType = Type.GetType(ReadAsString(reader));
                                break;
                            case Behavior:
                                if (behaviorType == null)
                                {
                                    throw new JsonException("Expecting behaviorType property before behavior.");
                                }

                                tknType = ReadNextToken(reader);
                                behavior = serializer.Deserialize(reader, behaviorType);
                                break;
                        }

                        tknType = ReadNextToken(reader);
                        break;
                    case JsonToken.EndObject:
                        done = true;
                        break;
                }
            }

            if (behavior == null || behaviorType == null || !isAsync.HasValue || selector == null)
            {
                throw new JsonException("Missing json properties to create the action.");
            }

            return CreateAction(behaviorType, behavior, isAsync.Value, selector);
        }

        private static object CreateAction(Type behaviorType, dynamic behavior, bool isAsync, string selector)
        {
            Type genericActionType;
            Type genericBehaviorType;
            if (isAsync)
            {
                genericBehaviorType = GenericActionBehaviorAsyncType;
                genericActionType = GenericAsyncActionType;
            }
            else
            {
                genericBehaviorType = GenericActionBehaviorType;
                genericActionType = GenericSyncActionType;
            }

            var behaviorInterface = behaviorType.GetInterfaces()
                .First(i => i.IsGenericType && ReferenceEquals(i.GetGenericTypeDefinition(), genericBehaviorType));

            var genArgs = behaviorInterface.GetGenericArguments();
            var appStateType = genArgs[0];
            var stateType = genArgs[1];

            var actionType = genericActionType.MakeGenericType(appStateType, stateType);

            var expressionParser = new ExpressionParser(parameterTypeResolver: new ParameterTypeResolver(appStateType));
            var selectorExpression = expressionParser.Parse(selector);

            return Activator.CreateInstance(actionType, behavior, selectorExpression);
        }

        private static string ReadAsString(JsonReader reader)
        {
            var val = reader.ReadAsString();
            if (val == null)
            {
                throw new JsonException($"Unable to read a string for {reader.Path}");
            }

            return val;
        }

        private static bool ReadAsBoolean(JsonReader reader)
        {
            var val = reader.ReadAsBoolean();
            if (!val.HasValue)
            {
                throw new JsonException($"Unable to read a boolean for {reader.Path}");
            }

            return val.Value;
        }

        private static JsonToken ReadNextToken(JsonReader reader)
        {
            if (!reader.Read())
            {
                throw new JsonException();
            }

            return reader.TokenType;
        }

        private class ParameterTypeResolver : IParameterTypeResolver
        {
            private Type appStateType;
            private string resolvedParameterName;

            public ParameterTypeResolver(Type appStateType)
            {
                this.appStateType = appStateType;
            }

            public Type ResolveType(string parameterName)
            {
                if (this.resolvedParameterName != null && this.resolvedParameterName != parameterName)
                {
                    throw new FormatException("Only one parameter is expected");
                }

                this.resolvedParameterName = parameterName;
                return this.appStateType;
            }
        }
    }
}