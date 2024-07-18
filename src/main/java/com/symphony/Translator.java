package com.symphony;

import lombok.extern.log4j.Log4j2;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Log4j2
public class Translator {

    private final String folderPath;
    private final ConfigurationLoader configuration;
    private final Statistics statistics = new Statistics();

    public Translator(String folderPath) {
        this.folderPath = folderPath;
        this.configuration = new ConfigurationLoader(folderPath);
    }

    public void process() throws IOException {
        configuration.load();

        List<Path> dataFiles;
        try (Stream<Path> paths = Files.list(Paths.get(folderPath))) {
            dataFiles = paths
                    .filter(path -> path.getFileName().toString().startsWith("dataFile") && path.toString().endsWith(".tsv"))
                    .toList();
        }

        if (dataFiles.isEmpty()) {
            log.error("No data files found in the specified folder.");
            return;
        }

        for (Path dataFile : dataFiles) {
            FileUtils.checkFileExists(dataFile);
        }

        try (ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors())) {
            List<Future<Void>> futures = new ArrayList<>();

            for (Path dataFile : dataFiles) {
                futures.add(executorService.submit(new DataFileProcessor(dataFile, configuration, statistics)));
            }

            for (Future<Void> future : futures) {
                future.get();
            }
        } catch (Exception e) {
            log.error("An error occurred while processing data files", e);
        }

        // Generate statistics
        generateStatistics();
    }

    private void generateStatistics() {
        log.info("Files processed successfully: {}", statistics.getProcessedFiles());
        log.info("Files failed: {}", statistics.getFailedFiles());
        log.info("Total rows: {}", statistics.getTotalRows());
        log.info("Processed rows: {}", statistics.getProcessedRows());
    }
}
