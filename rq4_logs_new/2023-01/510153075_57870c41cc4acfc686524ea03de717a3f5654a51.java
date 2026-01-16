package sim.bot.command;

import org.javacord.api.event.message.MessageCreateEvent;
import sim.bot.audio.SimPlayer;

import java.util.ArrayList;
import java.util.Random;

public class KeySpeakCmd implements Executable {
    private static final ArrayList<String> KEY_SAYINGS = new ArrayList<>() {{
        add("Life is ultimately meaningless, and our existence is nothing more than a fleeting moment in an infinite, uncaring universe.");
        add("The universe is a cold, empty void, and our actions are insignificant in the grand scheme of things. Our existence is a mere accident, and death is the only true certainty.");
        add("All human endeavors, beliefs, and emotions are ultimately futile, as we are just biological machines destined to return to dust.");
        add("In the face of an uncaring, infinite universe, human existence is a cruel joke, and all efforts to find purpose or meaning in life are in vain.");
        add("The ultimate truth is that we are nothing and our actions have no consequence. The universe will continue on long after we are gone, and our time here is nothing but a temporary blip in the grand scheme of things.");
        add("Life is a meaningless cycle of birth, suffering, and death. In the end, we will all be forgotten, and our time here will have been for nothing.");
        add("The world is indifferent to our struggles and suffering. We are alone in a meaningless existence, and our fate is ultimately determined by forces beyond our control.");
        add("In the end, all of our hopes, dreams, and accomplishments will be for naught, as death will ultimately claim us all, rendering our lives meaningless.");
        add("The only thing that is certain in life is that it will end. The universe will continue on without us and our time here is but a fleeting instant in the grand scheme of things.");
        add("Life is a meaningless journey towards a predetermined end.");
        add("The universe is vast and uncaring, and our existence is nothing more than a drop in the ocean.");
        add("The only thing that is truly certain in life is death, and all our efforts to achieve meaning and purpose are ultimately futile.");
        add("The world is a cruel and indifferent place, and our time here is but a fleeting moment in the grand scheme of things.");
        add("The universe is a cold, dark void, and our existence is nothing more than a temporary blip in the grand scheme of things.");
        add("In the face of an uncaring universe, human existence is a cruel joke, and all efforts to find purpose or meaning in life are in vain.");
        add("Life is a cycle of suffering, and all our hopes and dreams are nothing more than illusions.");
        add("In the end, all of our struggles and accomplishments will be for naught, as death will ultimately claim us all.");
        add("All human endeavors, beliefs, and emotions are ultimately futile, as we are just biological machines destined to return to dust.");
        add("The ultimate truth is that we are nothing and our actions have no consequence, the universe will continue on without us");
    }};

    @Override
    public void execute(SimPlayer player, MessageCreateEvent mce, ArrayList<String> args) {
        int index = (new Random()).nextInt(0, KEY_SAYINGS.size() + 1);
        mce.getMessage().reply(KEY_SAYINGS.get(index));
    }

    @Override
    public String help() {
        return "- [keyspeak | keyemulate | keysimulate | whatissomethingkeywouldsay]";
    }
}