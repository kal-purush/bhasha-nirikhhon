package tech.michalmaniak.Placeholder;
import me.clip.placeholderapi.expansion.PlaceholderExpansion;
import org.bukkit.OfflinePlayer;

public class SkillExpansion extends PlaceholderExpansion {

    @Override
    public boolean canRegister(){
        return true;
    }

    @Override
    public String getIdentifier(){
        return "crp";
    }

    @Override
    public String getAuthor(){
        return "michalmaniak";
    }

    @Override
    public String getVersion(){
        return "1.0.0";
    }

    @Override
    public String onRequest(OfflinePlayer player, String identifier){

        // %example_placeholder1%
        if(identifier.equals("placeholder1")){
            return "placeholder1 works";
        }

        // %example_placeholder2%
        if(identifier.equals("placeholder2")){
            return player.getName();
        }

        // We return null if an invalid placeholder (f.e. %example_placeholder3%)
        // was provided
        return null;
    }
}