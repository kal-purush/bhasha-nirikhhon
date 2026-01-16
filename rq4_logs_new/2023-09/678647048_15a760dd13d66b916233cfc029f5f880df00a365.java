package net.okuri.qol.event;

import net.okuri.qol.qolCraft.maturation.Maturationable;

import org.bukkit.block.Barrel;
import org.bukkit.block.Sign;
import org.bukkit.entity.Player;
import org.bukkit.event.Event;
import org.bukkit.event.HandlerList;
import org.bukkit.inventory.ItemStack;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;


public class MaturationEndEvent extends Event {
    private static final HandlerList HANDLERS = new HandlerList();
    private final Player player;
    private final Maturationable result;
    private final ArrayList<ItemStack> inputItems;
    private final Barrel barrel;
    private final Sign sign;
    private final LocalDateTime start;
    private final LocalDateTime end;
    private final double temp;
    private final double humid;

    public static HandlerList getHandlerList() {
        return HANDLERS;
    }



    public MaturationEndEvent(Player player, Maturationable result, ArrayList<ItemStack> inputItems, Barrel barrel, Sign sign, LocalDateTime start, LocalDateTime end, double temp, double humid) {
        this.player = player;
        this.result = result;
        this.inputItems = inputItems;
        this.barrel = barrel;
        this.sign = sign;
        this.start = start;
        this.end = end;
        this.temp = temp;
        this.humid = humid;
    }


    @Override
    public HandlerList getHandlers() {
        return HANDLERS;
    }
    public static HandlerList getHandlersList() {
        return HANDLERS;
    }

    public Player getPlayer() {
        return player;
    }
    public ArrayList<ItemStack> getIngredients() {
        return inputItems;
    }
    public Barrel getBarrel() {
        return barrel;
    }
    public LocalDateTime getStart() {
        return start;
    }
    public LocalDateTime getEnd() {
        return end;
    }
    public Duration getDuration(){
        return Duration.between(start, end);
    }
    public Maturationable getResult() {
        return result;
    }
    public double getTemp() {
        return temp;
    }
    public double getHumid() {
        return humid;
    }
    public Sign getSign() {
        return sign;
    }

}