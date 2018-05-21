package de.mlessmann.totaleconomy.worker;

import de.mlessmann.totaleconomy.except.AbortedException;
import ninja.leaping.configurate.ConfigurationNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spongepowered.api.text.Text;
import org.spongepowered.api.text.channel.MessageChannel;
import org.spongepowered.api.text.chat.ChatTypes;
import org.spongepowered.api.text.format.TextColors;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Created by MarkL4YG on 14-May-18
 */
public abstract class AbstractWorker implements Runnable {

    protected Logger logger = LoggerFactory.getLogger("ImportExport");
    protected final Object LOCK = new Object();

    private boolean isAborted;

    protected Consumer<AbstractWorker> onDone;
    protected MessageChannel feedback;

    protected ConfigurationNode mainConfig;
    protected ConfigurationNode accountsConfig;
    protected ConfigurationNode jobsConfig;

    protected DataSource dataSource;
    protected Connection dataConnection;

    protected AtomicInteger failures = new AtomicInteger(0);

    public AbstractWorker(MessageChannel feedback, Consumer<AbstractWorker> onDone) {
        this.feedback = feedback;
        this.onDone = onDone;
    }

    public void abort() {
        synchronized (LOCK) {
            isAborted = true;
        }
    }

    public boolean isAborted() {
        synchronized (LOCK) {
            return isAborted;
        }
    }

    protected void internalAbort() {
        if (!isAborted)
            abort();
        sendStatus("Working aborted!");
        cleanup();
        throw new AbortedException();
    }

    protected void internalAbort(Throwable e) {
        sendStatus("Working aborted due to exception! " + e.getClass().getSimpleName() + ": " + e.getMessage());
        cleanup();
    }

    protected void cleanup() {
        try {
            if (dataConnection != null && !dataConnection.isClosed())
                dataConnection.close();
        } catch (SQLException e) {
            logger.warn("Failed to close data connection", e);
        }
        if (onDone != null)
            onDone.accept(this);
        feedback.send(
            Text.of(TextColors.YELLOW,  '[', this.getClass().getSimpleName(), ']', TextColors.GOLD, "Worker exiting."),
            ChatTypes.SYSTEM
        );
    }

    public void sendStatus(String statusMessage) {
        feedback.send(
            Text.of(TextColors.YELLOW,  '[', this.getClass().getSimpleName(), ']', TextColors.GRAY, statusMessage),
            ChatTypes.SYSTEM
        );
    }
}
