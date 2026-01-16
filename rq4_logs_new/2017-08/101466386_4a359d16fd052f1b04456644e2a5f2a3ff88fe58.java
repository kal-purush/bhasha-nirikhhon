package io.github.eclipseironworks.mc.eclipseraces;

import io.github.eclipseironworks.mc.eclipseraces.Properties.Effects.IRacialEffect;
import io.github.eclipseironworks.mc.eclipseraces.events.RaceChangeEvent;
import org.spongepowered.api.Sponge;
import org.spongepowered.api.command.CommandSource;
import org.spongepowered.api.entity.living.player.Player;
import org.spongepowered.api.event.cause.Cause;
import org.spongepowered.api.text.Text;

import java.util.*;

public interface ECharacter
{
    UUID getUUID();
//    ICharacterHealth getHealth();
    Player getPlayer();
    Race getRace();
    void setRace(Race race, Cause cause);
    void apply(IRacialEffect e);
    void remove(IRacialEffect e);
    int getRaceID();
    boolean hasChosenCharacter();
    default boolean isReal()
    {
        return false;
    }

    class Hidden
    {
        protected static class DefaultCharacter implements ECharacter
        {
            protected UUID uuid;
            //protected ICharacterHealth health;
            protected Player player;
            protected Race race;
            protected int raceID;
            private Set<IRacialEffect> activeEffects = new HashSet<>();
            private boolean raceSetAtStart = false;


            private DefaultCharacter() { }
            protected DefaultCharacter(Builder builder)
            {
                this.player = builder.player;
                this.raceID = builder.raceID;
                this.uuid = builder.uuid;
            }

            @Override
            public boolean isReal()
            {
                return true;
            }
            @Override
            public UUID getUUID()
            {
                return uuid;
            }
/*
            @Override
            public ICharacterHealth getHealth()
            {
                return health;
            }
*/
            @Override
            public Player getPlayer()
            {
                return player;
            }
            @Override
            public Race getRace()
            {
                return race;
            }
            public int getRaceID()
            {
                return this.raceID;
            }

            @Override
            public boolean hasChosenCharacter()
            {
                return raceSetAtStart;
            }

            public void changeRace(Race newRace, Cause cause)
            {
                for(IRacialEffect e : activeEffects)
                {
                    this.remove(e);
                }
                for(IRacialEffect e : newRace.getEffects())
                {
                    this.apply(e);
                }
                Sponge.getEventManager().post(new RaceChangeEvent(this.player, this.race, newRace, cause));
                this.race = newRace;
                this.raceID = newRace.getRacialID();

            }

            @Override
            public void setRace(Race newRace, Cause cause)
            {
                Optional<RaceManager.RaceChangeReason> reason = cause.first(RaceManager.RaceChangeReason.class);
                if(reason.isPresent())
                    switch (reason.get())
                    {
                        case COMMAND:
                            if(raceSetAtStart)
                            {
                                player.sendMessage(Text.of("You may not change your race."));

                                return;
                            }
                            else
                            {
                                player.sendMessage(Text.of("A fine choice indeed."));
                                changeRace(newRace, cause);
                            }


                        case ADMIN_COMMAND:
                                Optional<CommandSource> sourceOptional = cause.first(CommandSource.class);
                                if (sourceOptional.isPresent())
                                {
                                    CommandSource source = sourceOptional.get();
                                    if (source instanceof Player) player.sendMessage(Text.of(((Player)source).getName() + " has changed your race."));

                                    else player.sendMessage(Text.of("Something has changed your race."));

                                    changeRace(newRace, cause);
                                }
                        case QUEST:
                            player.sendMessage(Text.of("How in the hell, this wasn't implemented."));
                    }



            }


            @Override
            public void apply(IRacialEffect e)
            {
                if(activeEffects.contains(e)) return;
                activeEffects.add(e);
                e.onApply(this.player);
            }
            public void remove(IRacialEffect e)
            {
                activeEffects.remove(e);
                e.onRemove(this.player);
            }

        }
    }

    static Builder builder()
    {
        return new Builder();
    }
    class Builder extends Hidden.DefaultCharacter
    {
        public Builder()
        {
        }

        public Builder uuid(UUID uuid)
        {
            this.uuid = uuid;
            return this;
        }

        public Builder player(Player p)
        {
            this.player = p;
            this.uuid = p.getUniqueId();
            return this;
        }
        /*
        public Builder health(ICharacterHealth health)
        {
            this.health = health;
            return this;
        }
        */
        public Builder race(Race race)
        {
            this.race = race;
            return this;
        }
        public Builder raceID(int i)
        {
            this.raceID = i;
            return this;
        }
        public ECharacter build()
        {
            return new Hidden.DefaultCharacter(this);
        }
    }

}