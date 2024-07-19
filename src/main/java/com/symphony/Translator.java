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
        try {
            configuration.load();
        } catch (IOException e) {
            log.error("Failed to load configuration files", e);
            return;
        }

        List<Path> dataFiles;
        try (Stream<Path> paths = Files.list(Paths.get(folderPath))) {
            dataFiles = paths
                    .filter(path -> path.getFileName().toString().startsWith("dataFile") && path.toString().endsWith(".tsv"))
                    .collect(Collectors.toList());
        }

        if (dataFiles.isEmpty()) {
            log.error("No data files found in the specified folder.");
            return;
        }

        dataFiles.sort(Comparator.comparingInt(this::extractFileNumber));

        List<Integer> missingFiles = findMissingFiles(dataFiles);
        if (!missingFiles.isEmpty()) {
            missingFiles.forEach(missingFile -> log.error("Missing data file: dataFile{}.tsv", missingFile));
        }

        try (ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<Void>> futures = new ArrayList<>();

            for (Path dataFile : dataFiles) {
                futures.add(executorService.submit(new DataFileProcessor(dataFile, configuration, statistics)));
            }

            for (Future<Void> future : futures) {
                try {
                    future.get();
                } catch (ExecutionException | InterruptedException e) {
                    log.error("An error occurred while processing a data file", e);
                    statistics.incrementFailedFiles();
                    Thread.currentThread().interrupt(); // Restore interrupted state
                }
            }
        }

        // Generate statistics
        generateStatistics();
    }

    private int extractFileNumber(Path path) {
        String fileName = path.getFileName().toString();
        String numberPart = fileName.replaceAll("[^0-9]", "");
        return numberPart.isEmpty() ? -1 : Integer.parseInt(numberPart);
    }

    private List<Integer> findMissingFiles(List<Path> dataFiles) {
        List<Integer> fileNumbers = dataFiles.stream()
                .parallel()
                .map(this::extractFileNumber)
                .filter(number -> number != -1)
                .sorted()
                .toList();

        List<Integer> missingFiles = new ArrayList<>();
        for (int i = 0; i < fileNumbers.size() - 1; i++) {
            int current = fileNumbers.get(i);
            int next = fileNumbers.get(i + 1);
            for (int j = current + 1; j < next; j++) {
                missingFiles.add(j);
            }
        }
        return missingFiles;
    }

    private void generateStatistics() {
        log.info("Files processed successfully: {}", statistics.getProcessedFiles());
        log.info("Files failed: {}", statistics.getFailedFiles());
        log.info("Total rows: {}", statistics.getTotalRows());
        log.info("Processed rows: {}", statistics.getProcessedRows());
    }
}
