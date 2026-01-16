/*
 * Copyright (c) 2017, Adam <Adam@sigterm.info>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 *    list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package sh.zoltus.test;

import lombok.Data;

@Data
public class HiscoreResult {
    private String player;
    private Skill combat;
    private Skill overall;
    private Skill attack;
    private Skill defence;
    private Skill strength;
    private Skill hitpoints;
    private Skill ranged;
    private Skill prayer;
    private Skill magic;
    private Skill cooking;
    private Skill woodcutting;
    private Skill fletching;
    private Skill fishing;
    private Skill firemaking;
    private Skill crafting;
    private Skill smithing;
    private Skill mining;
    private Skill herblore;
    private Skill agility;
    private Skill thieving;
    private Skill slayer;
    private Skill farming;
    private Skill runecraft;
    private Skill hunter;
    private Skill construction;
    private Skill leaguePoints;
    private Skill bountyHunterHunter;
    private Skill bountyHunterRogue;
    private Skill clueScrollAll;
    private Skill clueScrollBeginner;
    private Skill clueScrollEasy;
    private Skill clueScrollMedium;
    private Skill clueScrollHard;
    private Skill clueScrollElite;
    private Skill clueScrollMaster;
    private Skill lastManStanding;
    private Skill soulWarsZeal;
    private Skill riftsClosed;
    private Skill abyssalSire;
    private Skill alchemicalHydra;
    private Skill barrowsChests;
    private Skill bryophyta;
    private Skill callisto;
    private Skill cerberus;
    private Skill chambersOfXeric;
    private Skill chambersOfXericChallengeMode;
    private Skill chaosElemental;
    private Skill chaosFanatic;
    private Skill commanderZilyana;
    private Skill corporealBeast;
    private Skill crazyArchaeologist;
    private Skill dagannothPrime;
    private Skill dagannothRex;
    private Skill dagannothSupreme;
    private Skill derangedArchaeologist;
    private Skill generalGraardor;
    private Skill giantMole;
    private Skill grotesqueGuardians;
    private Skill hespori;
    private Skill kalphiteQueen;
    private Skill kingBlackDragon;
    private Skill kraken;
    private Skill kreearra;
    private Skill krilTsutsaroth;
    private Skill mimic;
    private Skill nex;
    private Skill nightmare;
    private Skill phosanisNightmare;
    private Skill obor;
    private Skill sarachnis;
    private Skill scorpia;
    private Skill skotizo;
    private Skill tempoross;
    private Skill gauntlet;
    private Skill corruptedGauntlet;
    private Skill theatreOfBlood;
    private Skill theatreOfBloodHardMode;
    private Skill thermonuclearSmokeDevil;
    private Skill tzKalZuk;
    private Skill tzTokJad;
    private Skill venenatis;
    private Skill vetion;
    private Skill vorkath;
    private Skill wintertodt;
    private Skill zalcano;
    private Skill zulrah;

    public Skill getSkill(HiscoreSkill skill) {
        return switch (skill) {
            case ATTACK -> attack;
            case DEFENCE -> defence;
            case STRENGTH -> strength;
            case HITPOINTS -> hitpoints;
            case RANGED -> ranged;
            case PRAYER -> prayer;
            case MAGIC -> magic;
            case COOKING -> cooking;
            case WOODCUTTING -> woodcutting;
            case FLETCHING -> fletching;
            case FISHING -> fishing;
            case FIREMAKING -> firemaking;
            case CRAFTING -> crafting;
            case SMITHING -> smithing;
            case MINING -> mining;
            case HERBLORE -> herblore;
            case AGILITY -> agility;
            case THIEVING -> thieving;
            case SLAYER -> slayer;
            case FARMING -> farming;
            case RUNECRAFT -> runecraft;
            case HUNTER -> hunter;
            case CONSTRUCTION -> construction;
            case LEAGUE_POINTS -> leaguePoints;
            case OVERALL -> overall;
            case BOUNTY_HUNTER_HUNTER -> bountyHunterHunter;
            case BOUNTY_HUNTER_ROGUE -> bountyHunterRogue;
            case CLUE_SCROLL_ALL -> clueScrollAll;
            case CLUE_SCROLL_BEGINNER -> clueScrollBeginner;
            case CLUE_SCROLL_EASY -> clueScrollEasy;
            case CLUE_SCROLL_MEDIUM -> clueScrollMedium;
            case CLUE_SCROLL_HARD -> clueScrollHard;
            case CLUE_SCROLL_ELITE -> clueScrollElite;
            case CLUE_SCROLL_MASTER -> clueScrollMaster;
            case LAST_MAN_STANDING -> lastManStanding;
            case SOUL_WARS_ZEAL -> soulWarsZeal;
            case RIFTS_CLOSED -> riftsClosed;
            case ABYSSAL_SIRE -> abyssalSire;
            case ALCHEMICAL_HYDRA -> alchemicalHydra;
            case BARROWS_CHESTS -> barrowsChests;
            case BRYOPHYTA -> bryophyta;
            case CALLISTO -> callisto;
            case CERBERUS -> cerberus;
            case CHAMBERS_OF_XERIC -> chambersOfXeric;
            case CHAMBERS_OF_XERIC_CHALLENGE_MODE -> chambersOfXericChallengeMode;
            case CHAOS_ELEMENTAL -> chaosElemental;
            case CHAOS_FANATIC -> chaosFanatic;
            case COMMANDER_ZILYANA -> commanderZilyana;
            case CORPOREAL_BEAST -> corporealBeast;
            case CRAZY_ARCHAEOLOGIST -> crazyArchaeologist;
            case DAGANNOTH_PRIME -> dagannothPrime;
            case DAGANNOTH_REX -> dagannothRex;
            case DAGANNOTH_SUPREME -> dagannothSupreme;
            case DERANGED_ARCHAEOLOGIST -> derangedArchaeologist;
            case GENERAL_GRAARDOR -> generalGraardor;
            case GIANT_MOLE -> giantMole;
            case GROTESQUE_GUARDIANS -> grotesqueGuardians;
            case HESPORI -> hespori;
            case KALPHITE_QUEEN -> kalphiteQueen;
            case KING_BLACK_DRAGON -> kingBlackDragon;
            case KRAKEN -> kraken;
            case KREEARRA -> kreearra;
            case KRIL_TSUTSAROTH -> krilTsutsaroth;
            case MIMIC -> mimic;
            case NEX -> nex;
            case NIGHTMARE -> nightmare;
            case PHOSANIS_NIGHTMARE -> phosanisNightmare;
            case OBOR -> obor;
            case SARACHNIS -> sarachnis;
            case SCORPIA -> scorpia;
            case SKOTIZO -> skotizo;
            case TEMPOROSS -> tempoross;
            case THE_GAUNTLET -> gauntlet;
            case THE_CORRUPTED_GAUNTLET -> corruptedGauntlet;
            case THEATRE_OF_BLOOD -> theatreOfBlood;
            case THEATRE_OF_BLOOD_HARD_MODE -> theatreOfBloodHardMode;
            case THERMONUCLEAR_SMOKE_DEVIL -> thermonuclearSmokeDevil;
            case TZKAL_ZUK -> tzKalZuk;
            case TZTOK_JAD -> tzTokJad;
            case VENENATIS -> venenatis;
            case VETION -> vetion;
            case VORKATH -> vorkath;
            case WINTERTODT -> wintertodt;
            case ZALCANO -> zalcano;
            case ZULRAH -> zulrah;
            case COMBAT -> combat;
        };
    }
}