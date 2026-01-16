/*
 * Copyright (c) 2018, GoMint, BlackyPaw and geNAZt
 *
 * This code is licensed under the BSD license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.gomint.plugin.scoreboard;

import io.gomint.ChatColor;
import io.gomint.GoMint;
import io.gomint.entity.EntityPlayer;
import io.gomint.plugin.TestPlugin;
import io.gomint.scoreboard.DisplayEntry;
import io.gomint.scoreboard.DisplaySlot;
import io.gomint.scoreboard.Scoreboard;
import io.gomint.scoreboard.ScoreboardDisplay;

import java.text.NumberFormat;
import java.util.concurrent.TimeUnit;

/**
 * @author geNAZt
 * @version 1.0
 */
public class DebugScoreboard {

    private final Scoreboard scoreboard;
    private final ScoreboardDisplay display;

    private DisplayEntry tpsEntry;

    // Performance caches
    private final NumberFormat format;
    private double oldTPS = 0;

    public DebugScoreboard( TestPlugin plugin ) {
        this.scoreboard = GoMint.instance().createScoreboard();
        this.display = this.scoreboard.addDisplay( DisplaySlot.SIDEBAR, "debug", ChatColor.GREEN + "Go" + ChatColor.GRAY + "Mint" );

        this.display.addLine( " ", 0 );
        this.display.addLine( ChatColor.GOLD + "TPS     ", 1 );
        this.tpsEntry = this.display.addLine( ChatColor.RED + "0.00", 2 );

        // Performance things
        this.format = NumberFormat.getNumberInstance();
        this.format.setMaximumFractionDigits( 2 );
        this.format.setMinimumFractionDigits( 2 );

        // Schedule updated for this scoreboard
        plugin.getScheduler().schedule( this::updateTPS, 1, 1, TimeUnit.MILLISECONDS );
    }

    private void updateTPS() {
        if ( this.oldTPS != GoMint.instance().getTPS() ) {
            // Remove the old entry
            this.display.removeEntry( this.tpsEntry );
            this.tpsEntry = this.display.addLine( ChatColor.GREEN + this.format.format( GoMint.instance().getTPS() ), 2 );
            this.oldTPS = GoMint.instance().getTPS();
        }
    }

    public void addPlayer( EntityPlayer player ) {
        player.setScoreboard( this.scoreboard );
    }

}