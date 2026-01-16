package com.pieisnotpi.engine.ui.text.effects;

import com.pieisnotpi.engine.ui.text.Text;

public class UnfoldEffect implements TextEffect
{
    private Text text;
    private int index;
    private float timePassed, speed;

    public UnfoldEffect(int charsPerSecond)
    {
        speed = 1f/charsPerSecond;
    }

    public void start()
    {
        onTextUpdated();
        text.getMesh().flagForBuild();
    }

    public void end()
    {
        text.chars.forEach(t -> t.enabled = true);
        text.getMesh().flagForBuild();
    }

    @Override
    public void setText(Text text)
    {
        this.text = text;
    }

    @Override
    public void process(float timeStep)
    {
        int steps = (int) Math.floor((timePassed += timeStep)/(speed*index));

        for(int toIndex = index + steps; index < text.chars.size() && index < toIndex; index++) text.chars.get(index).enabled = true;
    }

    @Override
    public void onTextUpdated()
    {
        for(int i = 1; i < text.chars.size(); i++) text.chars.get(i).enabled = false;
        timePassed = 0;
        index = 1;
    }
}