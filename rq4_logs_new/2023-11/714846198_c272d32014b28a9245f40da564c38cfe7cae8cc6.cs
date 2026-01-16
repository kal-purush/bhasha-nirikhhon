using System;
using TeamB_TD.Battle.Unit.Ally;

namespace TeamB_TD
{
    namespace Battle
    {
        namespace Craft
        {
            public static class Converter
            {
                public static AllyType ToAllyType(this CraftType craftType)
                {
                    switch (craftType)
                    {
                        case CraftType.Invalid: return AllyType.Invalid;
                        case CraftType.Flag: return AllyType.Flag;
                        case CraftType.Bomb: return AllyType.Bomb;
                        case CraftType.Horn: return AllyType.Horn;
                        case CraftType.Wall: return AllyType.Wall;
                        case CraftType.Portion: return AllyType.Portion;
                        case CraftType.Cannon: return AllyType.Cannon;
                        default: throw new ArgumentException(nameof(craftType));
                    }
                }
            }
        }
    }
}