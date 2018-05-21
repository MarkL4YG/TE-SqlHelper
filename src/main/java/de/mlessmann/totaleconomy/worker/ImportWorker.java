package de.mlessmann.totaleconomy.worker;

import de.mlessmann.totaleconomy.except.WorkerException;
import ninja.leaping.configurate.ConfigurationNode;
import ninja.leaping.configurate.hocon.HoconConfigurationLoader;
import org.spongepowered.api.Sponge;
import org.spongepowered.api.service.sql.SqlService;
import org.spongepowered.api.text.channel.MessageChannel;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.function.Consumer;

/**
 * Created by MarkL4YG on 14-May-18
 */
public class ImportWorker extends AbstractWorker {

    private File totalEconomyDir;

    private List<String> currencies;
    private List<String> jobs;

    public ImportWorker(File totalEconomyDir, MessageChannel feedback, Consumer<AbstractWorker> onDone) {
        super(feedback, onDone);
        this.totalEconomyDir = totalEconomyDir;
    }

    @Override
    public void run() {

        setupConfiguration();

        if (isAborted()) {
            internalAbort();
            return;
        }

        setupDataConnection();

        if (isAborted()) {
            internalAbort();
            return;
        }

        readColumns();

        if (isAborted()) {
            internalAbort();
            return;
        }

        migrateDown();

        if (isAborted()) {
            internalAbort();
            return;
        }

        migrateUp();

        if (isAborted()) {
            internalAbort();
            return;
        }

        importAccounts();

        sendStatus("Import successful!");
        cleanup();
    }

    private void setupConfiguration() {
        sendStatus("Reading TE configuration...");

        try {
            HoconConfigurationLoader mainLoader = HoconConfigurationLoader.builder()
                                                                          .setFile(new File(totalEconomyDir, "totaleconomy.conf"))
                                                                          .build();
            HoconConfigurationLoader accountsLoader = HoconConfigurationLoader.builder()
                                                                              .setFile(new File(totalEconomyDir, "accounts.conf"))
                                                                              .build();
            HoconConfigurationLoader jobsLoader = HoconConfigurationLoader.builder()
                                                                          .setFile(new File(totalEconomyDir, "jobs.conf"))
                                                                          .build();
            mainConfig = mainLoader.load();
            accountsConfig = accountsLoader.load();
            jobsConfig = jobsLoader.load();

        } catch (IOException e) {
            internalAbort(e);
            throw new WorkerException("Failed to read configuration!", e);
        }
    }

    private void setupDataConnection() {
        sendStatus("Initializing database connection...");

        String url = mainConfig.getNode("database", "url").getString("[]");
        String password = mainConfig.getNode("database", "password").getString("");
        String user = mainConfig.getNode("database", "user").getString("");

        if (url.contains("[") || url.contains("]") || password.isEmpty() || user.isEmpty()) {
            sendStatus("Database is not configured!");
            internalAbort();
            return;
        }

        String jdbcUrl = url + "?user=" + user + "&password=" + password;

        try {
            SqlService dbService = Sponge.getServiceManager().provideUnchecked(SqlService.class);
            dataSource = dbService.getDataSource(jdbcUrl);
            dataConnection = dataSource.getConnection();

        } catch (SQLException e) {
            internalAbort(e);
            throw new WorkerException("Failed to create data source!", e);
        }

        if (isAborted()) {
            internalAbort();
        }
    }

    private void readColumns() {
        sendStatus("Collecting currencies...");
        currencies = new ArrayList<>();
        accountsConfig.getChildrenMap().forEach((key, node) -> {
            if (node.hasMapChildren()) {

                node.getChildrenMap()
                    .entrySet()
                    .stream()
                    .filter(e -> ((String) e.getKey()).endsWith("-balance"))
                    .filter(e -> !currencies.contains(e.getKey()))
                    .forEach(e -> currencies.add(((String) e.getKey())));
            }
        });

        jobs = new ArrayList<>();
        sendStatus("Collecting jobs...");
        jobsConfig.getNode("jobs").getChildrenMap()
                  .forEach((k, v) -> jobs.add((String) k));
    }

    private void migrateDown() {
        sendStatus("Wiping tables...");
        try (Statement statement = dataConnection.createStatement()) {
            statement.addBatch("DROP TABLE IF EXISTS `levels`");
            statement.addBatch("DROP TABLE IF EXISTS `experience`");
            statement.addBatch("DROP TABLE IF EXISTS `virtual_accounts`");
            statement.addBatch("DROP TABLE IF EXISTS `accounts`");
            statement.executeBatch();

        } catch (SQLException e) {
            internalAbort(e);
            throw new WorkerException("Failed to wipe tables!", e);
        }
    }

    public void migrateUp() {
        sendStatus("Generating tables...");
        try (Statement statement = dataConnection.createStatement()) {

            // ACCOUNTS
            StringBuilder builder = new StringBuilder();
            builder.append("CREATE TABLE `accounts` (\n");
            currencies.stream()
                      .map(this::balColumn)
                      .forEach(builder::append);

            builder.append("`uid` VARCHAR(60) PRIMARY KEY,\n");
            builder.append("`job` VARCHAR(50),\n");
            builder.append("`job_notifications` TINYINT(3)\n");
            builder.append(")");

            statement.addBatch(builder.toString());

            // VIRTUAL_ACCOUNTS
            builder = new StringBuilder();
            builder.append("CREATE TABLE `virtual_accounts` (\n");
            currencies.stream()
                      .map(this::balColumn)
                      .forEach(builder::append);

            builder.append("`uid` VARCHAR(60)\n");
            builder.append(")");

            statement.addBatch(builder.toString());

            // EXPERIENCE
            builder = new StringBuilder();
            builder.append("CREATE TABLE `experience` (\n");
            jobs.stream()
                .map(this::jobColumn)
                .forEach(builder::append);
            builder.append("`uid` VARCHAR(60),\n");
            builder.append("FOREIGN KEY (`uid`) REFERENCES accounts(`uid`)\n");
            builder.append(")");

            statement.addBatch(builder.toString());

            // LEVELS
            builder = new StringBuilder();
            builder.append("CREATE TABLE `levels` (\n");
            jobs.stream()
                .map(this::jobColumn)
                .forEach(builder::append);
            builder.append("`uid` VARCHAR(60),\n");
            builder.append("FOREIGN KEY (`uid`) REFERENCES accounts(`uid`)\n");
            builder.append(")");

            statement.addBatch(builder.toString());

            statement.executeBatch();

        } catch (SQLException e) {
            internalAbort(e);
            throw new WorkerException("Failed to create tables!", e);
        }
    }

    private String balColumn(String currency_name) {
        currency_name = currency_name.replaceAll("-balance", "");
        return currency_name + "_balance DECIMAL(19,2),\n";
    }

    private String jobColumn(String job_name) {
        return job_name + " INT(10) UNSIGNED,\n";
    }

    private void importAccounts() {
        sendStatus("Importing data... (this may take a moment)");

        try {
            Statement accountStatement = dataConnection.createStatement();

            List<Map.Entry<String, ConfigurationNode>> accounts =
              Arrays.asList(accountsConfig.getChildrenMap().entrySet().toArray(new Map.Entry[0]));

            accounts.stream()
                    .filter(entry -> entry.getValue().hasMapChildren())
                    .forEach(entry -> {
                        try {
                            accountStatement.addBatch(importAccountStatement(entry));
                            logger.debug("Import: " + entry.getKey());
                        } catch (SQLException e) {
                            failures.incrementAndGet();
                            logger.warn("Failed to import: " + entry.getKey(), e);
                        }
                      }
                    );

            accountStatement.executeLargeBatch();

            Statement expStatement = dataConnection.createStatement();

            accounts.stream()
                    .filter(entry -> entry.getValue().hasMapChildren())
                    .forEach(entry -> {
                        try {
                            String query = importExpStatement(entry);
                            if (query != null) {
                                expStatement.addBatch(query);
                                logger.debug("Import: " + entry.getKey());
                            }
                        } catch (SQLException e) {
                            failures.incrementAndGet();
                            logger.warn("Failed to import: " + entry.getKey(), e);
                        }
                    });

            expStatement.executeLargeBatch();

            Statement levelStatement = dataConnection.createStatement();

            accounts.stream()
                    .filter(entry -> entry.getValue().hasMapChildren())
                    .forEach(entry -> {
                        try {
                            String query = importLevelStatement(entry);
                            if (query != null) {
                                levelStatement.addBatch(query);
                                logger.debug("Import: " + entry.getKey());
                            }
                        } catch (SQLException e) {
                            failures.incrementAndGet();
                            logger.warn("Failed to import: " + entry.getKey(), e);
                        }
                    });

            levelStatement.executeLargeBatch();

        } catch (SQLException e) {
            internalAbort(e);
            throw new WorkerException("Failed to import accounts and balances", e);
        }
    }

    private String importAccountStatement(Map.Entry<String, ConfigurationNode> entry) {

        String table;
        String uid;
        try {
            uid = UUID.fromString(entry.getKey()).toString();
            table = "accounts";
        } catch (IllegalArgumentException e) {
            // Virtual account is default;
            uid = entry.getKey();
            table = "virtual_accounts";
        }

        List<String> columns = new LinkedList<>();
        List<String> values = new LinkedList<>();

        entry.getValue().getChildrenMap().values()
             .stream()
             .filter(v -> v.getKey() instanceof String)
             .filter(v -> ((String) v.getKey()).endsWith("-balance"))
             .forEach(v -> {
                 String key = ((String) v.getKey());
                 columns.add(key.substring(0, key.length()-8));
                 values.add(v.getValue(0).toString());
             });

        return toInsertQuery(table, uid, columns, values, "_balance");
    }

    private String importExpStatement(Map.Entry<String, ConfigurationNode> entry) {

        String table = "experience";
        String uid;
        try {
            uid = UUID.fromString(entry.getKey()).toString();
        } catch (IllegalArgumentException e) {
            return null;
        }

        ConfigurationNode statsNode = entry.getValue().getNode("jobstats");
        if (statsNode.isVirtual())
            return null;

        List<String> columns = new LinkedList<>();
        List<String> values = new LinkedList<>();
        statsNode.getChildrenMap().values()
                 .stream()
                 .filter(v -> v.getKey() instanceof String)
                 .forEach(v -> {
                     columns.add(((String) v.getKey()));
                     values.add(v.getNode("exp").getValue().toString());
                 });

        return toInsertQuery(table, uid, columns, values, "");
    }

    private String importLevelStatement(Map.Entry<String, ConfigurationNode> entry) {

        String table = "levels";
        String uid;
        try {
            uid = UUID.fromString(entry.getKey()).toString();
        } catch (IllegalArgumentException e) {
            return null;
        }

        ConfigurationNode statsNode = entry.getValue().getNode("jobstats");
        if (statsNode.isVirtual())
            return null;

        List<String> columns = new LinkedList<>();
        List<String> values = new LinkedList<>();
        statsNode.getChildrenMap().values()
                 .stream()
                 .filter(v -> v.getKey() instanceof String)
                 .forEach(v -> {
                     columns.add(((String) v.getKey()));
                     values.add(v.getNode("level").getValue().toString());
                 });

        return toInsertQuery(table, uid, columns, values, "");
    }

    private String toInsertQuery(String table, String uid, List<String> columns, List<String> values, String colSuffix) {
        StringBuilder colString = new StringBuilder("(");
        columns.stream()
               .map(s -> '`' + s + colSuffix + "`,")
               .forEach(colString::append);
        colString.append("`uid`)");

        StringBuilder valString = new StringBuilder("(");
        values.stream()
              .map(s -> "'" + s + "',")
              .forEach(valString::append);
        valString.append("'").append(uid).append("')");

        return "INSERT INTO `" + table + "` " + colString.toString()
               + " VALUES " + valString.toString();
    }
}
