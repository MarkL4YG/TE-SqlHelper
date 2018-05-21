package de.mlessmann.totaleconomy.worker;

import org.spongepowered.api.text.channel.MessageChannel;

import java.io.File;
import java.util.function.Consumer;

/**
 * Created by MarkL4YG on 14-May-18
 */
public class ExportWorker extends AbstractWorker {

    private File outputDir;

    public ExportWorker(File outputDir, MessageChannel feedback, Consumer<AbstractWorker> onDone) {
        super(feedback, onDone);
        this.outputDir = outputDir;
    }

    @Override
    public void run() {
        internalAbort();
    }
}
