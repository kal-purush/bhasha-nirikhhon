package skadic.commands;


import skadic.commands.limiters.ILimiter;
import skadic.commands.limiters.PermissionLimiter;
import skadic.commands.util.Utils;
import sx.blah.discord.handle.obj.IChannel;
import sx.blah.discord.util.EmbedBuilder;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static skadic.commands.util.Utils.sendMessage;


/**
 * Instantiate this to create your Command and register them to a {@link CommandRegistry} object
 * Annotate every subclass with the {@link Help} annotation to give them a help text
 */
@Help
public abstract class Command{

    protected final Set<ILimiter> limiters;
    protected CommandRegistry registry;
    protected Set<CommandTuple> subCommands;

    public Command(CommandRegistry registry, Set<ILimiter> limiters) {
        this.limiters = limiters;
        this.registry = registry;
        subCommands = new HashSet<>();
        if(!getLimiter(PermissionLimiter.class).isPresent())
            limiters.add(new PermissionLimiter(Permission.LOW, registry));
    }

    public Command(CommandRegistry registry, ILimiter... limiters) {
        this(registry, Arrays.stream(limiters).collect(Collectors.toSet()));
    }

    public final void addSubCommand(SubCommand subCommand, String name, String... aliases){
        CommandTuple tuple = new CommandTuple(subCommand, name, aliases);
        subCommands.add(tuple);
        registry.register(subCommand, name, aliases);
    }

    public final Set<CommandTuple> getSubCommands() {
        return subCommands;
    }

    public final boolean isSubCommand(String name){
        if(!hasSubCommands()) return false;
        for (CommandTuple subCommand : subCommands) {
            if(subCommand.getName().equalsIgnoreCase(name)) return true;
            for (String[] aliases : subCommands.stream().map(CommandTuple::getAliases).collect(Collectors.toSet())) {
                for (String alias : aliases) {
                    if(name.equalsIgnoreCase(alias)) return true;
                }
            }
        }
        return false;
    }

    public final Optional<Command> getSubCommand(String name){
        for (CommandTuple subCommand : subCommands) {
            if(subCommand.getName().equalsIgnoreCase(name)) return Optional.of(subCommand.getCommand());
        }
        return Optional.empty();
    }

    public final boolean hasSubCommands(){
        return !subCommands.isEmpty();
    }

    public final Set<ILimiter> getLimiters() {
        return limiters;
    }

    public final void addLimiter(ILimiter limiter){
        limiters.add(limiter);
    }

    @SuppressWarnings("unchecked cast")
    public final  <T extends ILimiter> Optional<T> getLimiter(Class<? extends T> limiterType){
        for (ILimiter limiter : limiters) {
            if(limiterType.isInstance(limiter))
                return Optional.of((T) limiter);
        }
        return Optional.empty();
    }

    public CommandRegistry getRegistry(){
        return registry;
    }

    public final void execute(CommandContext ctx){
        if(!executeCommand(ctx)) Utils.sendMessage(ctx.getChannel(), "Something went wrong :thinking:");
    }

    /**
     * command execution only to be used in the execute method. Not recommended to be made public
     *
     * @param ctx the Command Context
     * @return returns if the command has been executed successfully
     */
    protected abstract boolean executeCommand(CommandContext ctx);

    protected static class CommandTuple{
        private SubCommand command;
        private String name;
        private String[] aliases;

        public CommandTuple(SubCommand command, String name, String[] aliases) {
            this.command = command;
            this.name = name;
            this.aliases = aliases;
        }

        public Command getCommand() {
            return command;
        }

        public void setCommand(SubCommand command) {
            this.command = command;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String[] getAliases() {
            return aliases;
        }

        public void setAliases(String[] aliases) {
            this.aliases = aliases;
        }
    }
}