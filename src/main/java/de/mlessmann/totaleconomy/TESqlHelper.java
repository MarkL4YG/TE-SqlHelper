package de.mlessmann.totaleconomy;

import de.mlessmann.totaleconomy.commands.ExportCommand;
import de.mlessmann.totaleconomy.commands.ImportCommand;
import de.mlessmann.totaleconomy.except.WorkerException;
import de.mlessmann.totaleconomy.worker.AbstractWorker;
import de.mlessmann.totaleconomy.worker.ExportWorker;
import de.mlessmann.totaleconomy.worker.ImportWorker;
import org.slf4j.Logger;
import org.spongepowered.api.Sponge;
import org.spongepowered.api.command.args.GenericArguments;
import org.spongepowered.api.command.spec.CommandSpec;
import org.spongepowered.api.config.ConfigDir;
import org.spongepowered.api.event.Listener;
import org.spongepowered.api.event.Order;
import org.spongepowered.api.event.game.state.GameInitializationEvent;
import org.spongepowered.api.event.game.state.GameStoppingEvent;
import org.spongepowered.api.plugin.Dependency;
import org.spongepowered.api.plugin.Plugin;
import org.spongepowered.api.text.Text;
import org.spongepowered.api.text.channel.MessageChannel;

import javax.inject.Inject;
import java.io.File;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by MarkL4YG on 14-May-18
 */
@Plugin(
    id = "te-sqlhelper",
    name = "TE-SqlHelper",
    dependencies = {@Dependency(id = "totaleconomy", optional = true)})
public class TESqlHelper {

    @Inject
    public Logger logger;

    @Inject
    @ConfigDir(sharedRoot = false)
    public File configDirectory;

    private File totalEconomyDir;

    private ExecutorService executor = Executors.newSingleThreadExecutor();

    private AbstractWorker worker;

    private boolean locked = false;

    @Listener(order = Order.LATE)
    public void onInitialize(GameInitializationEvent event) {

        totalEconomyDir = new File(configDirectory.getParentFile(), "totaleconomy");

        if (!totalEconomyDir.isDirectory() || !totalEconomyDir.canRead()) {
            logger.warn("TotalEconomy was not found! Plugin will be disabled disabled");
            return;
        }

        logger.info("TotalEconomy found! Registering.");

        CommandSpec helperCommand = CommandSpec.builder()
                                               .description(Text.of("Base command"))
                                               .arguments(GenericArguments.none())
                                               .child(ImportCommand.commandSpec(this), "import")
                                               .child(ExportCommand.commandSpec(this), "export")
                                               .build();

        Sponge.getCommandManager().register(this, helperCommand, "te-helper");
    }

    @Listener
    public void onStop(GameStoppingEvent event) {
        worker.abort();

        try {
            Thread.sleep(5000);
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
            executor.shutdownNow();
        } catch (InterruptedException e) {
            logger.error("Failed to stop running task: ", e);
        }
    }

    public void exportTo(File directory, MessageChannel feedback) {
        if (locked)
            throw new WorkerException("A process is still running!");
        locked = true;

        worker = new ExportWorker(directory, feedback, this::done);
        executor.execute(worker);
    }

    public void importFrom(MessageChannel feedback) {
        if (locked)
            throw new WorkerException("A process is still running!");
        locked = true;

        worker = new ImportWorker(totalEconomyDir, feedback, this::done);
        executor.execute(worker);
    }

    public void done(AbstractWorker worker) {
        if (this.worker == worker)
            locked = false;
    }
}
