package de.mlessmann.totaleconomy.commands;

import de.mlessmann.totaleconomy.TESqlHelper;
import org.spongepowered.api.command.CommandException;
import org.spongepowered.api.command.CommandResult;
import org.spongepowered.api.command.CommandSource;
import org.spongepowered.api.command.args.CommandContext;
import org.spongepowered.api.command.args.GenericArguments;
import org.spongepowered.api.command.spec.CommandExecutor;
import org.spongepowered.api.command.spec.CommandSpec;
import org.spongepowered.api.text.Text;

/**
 * Created by MarkL4YG on 14-May-18
 */
public class ImportCommand implements CommandExecutor {

    public static CommandSpec commandSpec(TESqlHelper plugin) {
        return CommandSpec.builder()
                          .permission("totaleconomy.sqlhelper.import")
                          .arguments(GenericArguments.none())
                          .description(Text.of("Allows import of TotalEconomy data into a database"))
                          .executor(new ImportCommand(plugin))
                          .build();
    }

    private TESqlHelper plugin;

    public ImportCommand(TESqlHelper plugin) {
        this.plugin = plugin;
    }

    @Override
    public CommandResult execute(CommandSource src, CommandContext args) {
        plugin.importFrom(src.getMessageChannel());
        return CommandResult.success();
    }
}
