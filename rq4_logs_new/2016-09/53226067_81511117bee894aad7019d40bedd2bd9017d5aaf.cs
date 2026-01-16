using System;
using System.Collections.Generic;
using CardGameConsoleTestApp.Model.Cards.Interfaces;

namespace CardGameConsoleTestApp.Model.Engine
{
    public class GameEventManager
    {
        public static void Initialize()
        {
            TurnStart += OnTurnStart;
            onTurnStartListeners = new List<Tuple<ICard, TurnStartHandler>>();

            TurnEnd += OnTurnEnd;
            onTurnEndListeners = new List<Tuple<ICard, TurnEndHandler>>();

            CardDrawn += OnCardDrawn;
            onCardDrawnListeners = new List<Tuple<ICard, CardDrawnHandler>>();

            Attack += OnAttack;
            onAttackListeners = new List<Tuple<ICard, AttackEventHandler>>();

            OtherAttack += OnOtherAttack;
            onOtherAttackListeners = new List<Tuple<ICard, OtherAttackEventHandler>>();

            Healed += OnOtherHealed;
            onHealedListeners = new List<Tuple<ICard, HealedEventHandler>>();

            GetHit += OnGetHit;
            onGetHitListeners = new List<Tuple<ICard, GetHitEventHandler>>();

            OtherGetHit += OnOtherGetHit;
            onOtherGetHitListeners = new List<Tuple<ICard, OtherGetHitEventHandler>>();

            Death += OnDeath;
            onDeathEventListeners = new List<Tuple<ICard, DeathEventHandler>>();

            OtherDeath += OnOtherDeath;
            onOtherDeathEventListeners =new List<Tuple<ICard, OtherDeathEventHandler>>();

            CardPlayed += OnCardPlayed;
            onCardPlayedListeners = new List<Tuple<ICard, CardPlayedHandler>>();

            OtherCardPlayed += OnOtherCardPlayed;
            onOtherCardPlayedListeners = new List<Tuple<ICard, OtherCardPlayedHandler>>();

            SpellCast += OnSpellCast;
            onSpellCastListeners = new List<Tuple<ICard, SpellCastHandler>>();
        }

        public static void UnregisterForEvents(ICard card)
        {
            onTurnStartListeners.RemoveAll(c => c.Item1.Id == card.Id);
            onTurnEndListeners.RemoveAll(c => c.Item1.Id == card.Id);
            onCardDrawnListeners.RemoveAll(c => c.Item1.Id == card.Id);
            onAttackListeners.RemoveAll(c => c.Item1.Id == card.Id);
            onOtherAttackListeners.RemoveAll(c => c.Item1.Id == card.Id);
            onHealedListeners.RemoveAll(c => c.Item1.Id == card.Id);
            onOtherHealedListeners.RemoveAll(c => c.Item1.Id == card.Id);
            onGetHitListeners.RemoveAll(c => c.Item1.Id == card.Id);
            onOtherGetHitListeners.RemoveAll(c => c.Item1.Id == card.Id);
            onDeathEventListeners.RemoveAll(c => c.Item1.Id == card.Id);
            onOtherDeathEventListeners.RemoveAll(c => c.Item1.Id == card.Id);
            onCardPlayedListeners.RemoveAll(c => c.Item1.Id == card.Id);
            onOtherCardPlayedListeners.RemoveAll(c => c.Item1.Id == card.Id);
            onSpellCastListeners.RemoveAll(c => c.Item1.Id == card.Id);
        }

        public delegate void TurnStartHandler();

        public static TurnStartHandler TurnStart;
        internal static List<Tuple<ICard, TurnStartHandler>> onTurnStartListeners;

        public delegate void TurnEndHandler();

        public static TurnEndHandler TurnEnd;
        internal static List<Tuple<ICard, TurnEndHandler>> onTurnEndListeners;

        public delegate void CardDrawnHandler();

        public static CardDrawnHandler CardDrawn;
        internal static List<Tuple<ICard, CardDrawnHandler>> onCardDrawnListeners;

        public delegate void AttackEventHandler();

        public static AttackEventHandler Attack;
        internal static List<Tuple<ICard, AttackEventHandler>> onAttackListeners;

        public delegate void OtherAttackEventHandler();

        public static OtherAttackEventHandler OtherAttack;
        internal static List<Tuple<ICard, OtherAttackEventHandler>> onOtherAttackListeners;

        public delegate void HealedEventHandler();

        public static HealedEventHandler Healed;
        internal static List<Tuple<ICard, HealedEventHandler>> onHealedListeners;

        public delegate void OtherHealedEventHandler();

        public static OtherHealedEventHandler OtherHealed;
        internal static List<Tuple<ICard, OtherHealedEventHandler>> onOtherHealedListeners;

        public delegate void GetHitEventHandler();

        public static GetHitEventHandler GetHit;
        internal static List<Tuple<ICard, GetHitEventHandler>> onGetHitListeners;

        public delegate void OtherGetHitEventHandler();

        public static OtherGetHitEventHandler OtherGetHit;
        internal static List<Tuple<ICard, OtherGetHitEventHandler>> onOtherGetHitListeners;

        public delegate void DeathEventHandler();

        public static DeathEventHandler Death;
        internal static List<Tuple<ICard, DeathEventHandler>> onDeathEventListeners;

        public delegate void OtherDeathEventHandler();

        public static OtherDeathEventHandler OtherDeath;
        internal static List<Tuple<ICard, OtherDeathEventHandler>> onOtherDeathEventListeners;

        public delegate void CardPlayedHandler();

        public static CardPlayedHandler CardPlayed;
        internal static List<Tuple<ICard, CardPlayedHandler>> onCardPlayedListeners;

        public delegate void OtherCardPlayedHandler();

        public static OtherCardPlayedHandler OtherCardPlayed;
        internal static List<Tuple<ICard, OtherCardPlayedHandler>> onOtherCardPlayedListeners;

        public delegate void SpellCastHandler();

        public static SpellCastHandler SpellCast;
        internal static List<Tuple<ICard, SpellCastHandler>> onSpellCastListeners;



        //Register for events
        public static void RegisterForEvent(ICard self, TurnStartHandler callback)
        {
            onTurnStartListeners.Add(new Tuple<ICard, TurnStartHandler>(self, callback));
        }

        public static void RegisterForEvent(ICard self, TurnEndHandler callback)
        {
            onTurnEndListeners.Add(new Tuple<ICard, TurnEndHandler>(self, callback));
        }

        public static void RegisterForEvent(ICard self, CardDrawnHandler callback)
        {
            onCardDrawnListeners.Add(new Tuple<ICard, CardDrawnHandler>(self, callback));
        }

        public static void RegisterForEvent(ICard self, AttackEventHandler callback)
        {
            onAttackListeners.Add(new Tuple<ICard, AttackEventHandler>(self, callback));
        }

        public static void RegisterForEvent(ICard self, OtherAttackEventHandler callback)
        {
            onOtherAttackListeners.Add(new Tuple<ICard, OtherAttackEventHandler>(self, callback));
        }

        public static void RegisterForEvent(ICard self, HealedEventHandler callback)
        {
            onHealedListeners.Add(new Tuple<ICard, HealedEventHandler>(self, callback));
        }

        public static void RegisterForEvent(ICard self, OtherHealedEventHandler callback)
        {
            onOtherHealedListeners.Add(new Tuple<ICard, OtherHealedEventHandler>(self, callback));
        }

        public static void RegisterForEvent(ICard self, GetHitEventHandler callback)
        {
            onGetHitListeners.Add(new Tuple<ICard, GetHitEventHandler>(self, callback));
        }

        public static void RegisterForEvent(ICard self, OtherGetHitEventHandler callback)
        {
            onOtherGetHitListeners.Add(new Tuple<ICard, OtherGetHitEventHandler>(self, callback));
        }

        public static void RegisterForEvent(ICard self, DeathEventHandler callback)
        {
            onDeathEventListeners.Add(new Tuple<ICard, DeathEventHandler>(self, callback));
        }

        public static void RegisterForEvent(ICard self, OtherDeathEventHandler callback)
        {
            onOtherDeathEventListeners.Add(new Tuple<ICard, OtherDeathEventHandler>(self, callback));
        }

        public static void RegisterForEvent(ICard self, CardPlayedHandler callback)
        {
            onCardPlayedListeners.Add(new Tuple<ICard, CardPlayedHandler>(self, callback));
        }

        public static void RegisterForEvent(ICard self, OtherCardPlayedHandler callback)
        {
            onOtherCardPlayedListeners.Add(new Tuple<ICard, OtherCardPlayedHandler>(self, callback));
        }

        public static void RegisterForEvent(ICard self, SpellCastHandler callback)
        {
            onSpellCastListeners.Add(new Tuple<ICard, SpellCastHandler>(self, callback));
        }


        //Event Handlers
        public static void OnTurnStart()
        {
            
        }

        public static void OnTurnEnd()
        {
            
        }

        public static void OnCardDrawn()
        {
            
        }

        public static void OnAttack()
        {
            
        }

        public static void OnOtherAttack()
        {
            
        }

        public static void OnHealed()
        {
            
        }

        public static void OnOtherHealed()
        {
            
        }

        public static void OnGetHit()
        {
            
        }

        public static void OnOtherGetHit()
        {
            
        }

        public static void OnDeath()
        {
            
        }

        public static void OnOtherDeath()
        {
            
        }

        public static void OnCardPlayed()
        {
            
        }

        public static void OnOtherCardPlayed()
        {
            
        }

        public static void OnSpellCast()
        {
            
        }
    }
}