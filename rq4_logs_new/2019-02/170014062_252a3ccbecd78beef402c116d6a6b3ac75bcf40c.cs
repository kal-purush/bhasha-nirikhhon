using Microsoft.Bot.Builder;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;

namespace HackedBrain.BotBuilder.Integration.AspNet.Core
{
    public class BotStateBuilder<TBotState>
        where TBotState : BotState
    {
        private readonly BotStateConfigurationBuilder _botStateConfigurationBuilder;
        private Func<TBotState> _botStateFactory;
        private IStorage _storage;
        private Dictionary<string, Type> _properties;

        internal BotStateBuilder(BotStateConfigurationBuilder botStateConfigurationBuilder)
        {
            _botStateConfigurationBuilder = botStateConfigurationBuilder ?? throw new ArgumentNullException(nameof(botStateConfigurationBuilder));
            _storage = null;
            _properties = new Dictionary<string, Type>();
        }

        public BotStateBuilder<TBotState> UseBotStateFactory(Func<TBotState> factory)
        {
            _botStateFactory = factory ?? throw new ArgumentNullException(nameof(factory));

            return this;
        }

        public BotStateBuilder<TBotState> UseStorage(IStorage storage)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));

            return this;
        }

        public BotStateBuilder<TBotState> WithProperty<TValue>(string name = null)
        {
            _properties.Add(name ?? typeof(TValue).Name, typeof(TValue));

            return this;
        }

        internal BotState Build()
        {
            // WARNING: taming of reflection dragons ahead

            var botState = default(BotState);

            if(_botStateFactory == null)
            {
                botState = (BotState)Activator.CreateInstance(typeof(TBotState), _storage);
            }
            else
            {
                botState = _botStateFactory.Invoke();

                if(botState == default(BotState))
                {
                    throw new InvalidOperationException($"The specified bot state factory returned a null {nameof(BotState)} instance.");
                }
            }

            // TODO: should be an overload of CreateProperty that takes Type!
            var createPropertyGenericMethodInfo = typeof(BotState).GetMethod("CreateProperty").GetGenericMethodDefinition();
            var statePropertyAccessorGenericInterface = typeof(IStatePropertyAccessor<>);

            var services = _botStateConfigurationBuilder.Services;

            foreach (var entry in _properties)
            {
                var createPropertyMethodInfo = createPropertyGenericMethodInfo.MakeGenericMethod(entry.Value);

                var propertyAccessor = createPropertyMethodInfo.Invoke(botState, new[] { entry.Key });

                services.AddSingleton(statePropertyAccessorGenericInterface.MakeGenericType(entry.Value), propertyAccessor);
            }

            _botStateConfigurationBuilder.UseBotState(botState);

            return botState;
        }
    }
}