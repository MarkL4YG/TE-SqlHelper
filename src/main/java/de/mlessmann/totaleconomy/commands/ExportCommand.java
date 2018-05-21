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
public class ExportCommand implements CommandExecutor {

    public static CommandSpec commandSpec(TESqlHelper plugin) {
        return CommandSpec.builder()
                          .permission("totaleconomy.sqlhelper.export")
                          .arguments(GenericArguments.optional(GenericArguments.string(Text.of(""))))
                          .description(Text.of("Exports database contents into a file configuration"))
                          .executor(new ExportCommand(plugin))
                          .build();
    }

    private TESqlHelper helper;

    public ExportCommand(TESqlHelper helper) {
        this.helper = helper;
    }

    @Override
    public CommandResult execute(CommandSource src, CommandContext args) {
        return null;
    }
}
