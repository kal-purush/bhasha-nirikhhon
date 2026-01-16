using System.Collections.Generic;
using System.Linq;
using CardGame.DBML;
using CardGame.Model.Cards;
using CardGame.Model.Cards.ValueObjects;
using CardTrigger = CardGame.Model.Cards.ValueObjects.CardTrigger;

namespace CardGame.Controller
{
    public class CardController
    {
        private static CardController _cardController;

        private CardController()
        {
        }

        public static CardController GetInstance()
        {
            return _cardController ?? (_cardController = new CardController());
        }

        public IList<Minion> GetAllMinionsList(int languageId)
        {
            using (var context = new CardDataContext())
            {
                var minions = from c in context.Cards
                    join n in context.CardNames on c.Id equals n.CardId
                    where c.Type == (int) CardType.Minion
                          && n.LanguageId == languageId
                    select new Minion()
                    {
                        Name = n.Name,
                        Cost = c.Cost,
                        Attack = c.Attack,
                        Health = c.Health,
                        CurrentHealth = c.Health,
                        Triggers = new List<CardTrigger>
                        {
                            new CardTrigger
                            {
                                Type = TriggerType.OnTurnStart,
                                MethodName = "DealDamage",
                                MethodClass = "Triggers",
                                MethodParams = new List<TriggerMethodParam>
                                {
                                    new TriggerMethodParam
                                    {
                                        ParamName = "target",
                                        ParamValue = null
                                    },
                                    new TriggerMethodParam
                                    {
                                        ParamName = "damage",
                                        ParamValue = "2"
                                    }
                                }
                            }
                        }
                    };
                                 

                return minions.ToList();
            }
        }

        public IList<Spell> GetAllSpellsList(int languageId)
        {
            //using (var context = new CardDataContext())
            //{
            //    var spells = (from c in context.Cards
            //        join n in context.CardNames on c.Id equals n.CardId
            //        where c.Type == (int) CardType.Spell
            //        && n.LanguageId == languageId
            //        select new Spell()
            //        {
            //            Name = n.Name,
            //            Cost = c.Cost
            //        });

            //    return spells.ToList();
            //}
            return null;
        }

        public void LoadTriggerItem()
        {
            //using (var context = new CardDataContext())
            //{
            //    var x = (from c in context.Cards
            //        join n in context.CardNames on c.Id equals n.CardId
            //        join tr in context.CardTriggers on c.Id equals tr.CardId
            //        where c.Id == triggerable.Id
            //        select new
            //    {
            //        Id = c.Id,
            //        Name = n.Name,
            //        Trigger = (TriggerType)tr.TriggerType,
            //        Method = tr.Method.Name,
            //        MethodParams = (from tp in context.CardTriggerParams
            //                        where tp.CardTriggerId == tr.Id
            //                        select new
            //                        {
            //                            tp.ParamName,
            //                            tp.ParamValue
            //                        }).AsEnumerable()
            //    });

            //    foreach (var y in x)
            //    {
            //        var paramsera = y.MethodParams.ToDictionary(t => t.ParamName, t => t.ParamValue);
            //        Console.WriteLine($"{y.Id}, {y.Name}, {y.Trigger}, {y.Method}");
            //    }
            //}
        }
    }
}