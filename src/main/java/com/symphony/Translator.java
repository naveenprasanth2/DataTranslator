package com.symphony;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class Translator {
    private final String folderPath;
    private final ConfigurationLoader configuration;
    private final Statistics statistics = new Statistics();

    public Translator(String folderPath) {
        this.folderPath = folderPath;
        this.configuration = new ConfigurationLoader(folderPath);
    }

    public void process() throws IOException, InterruptedException, ExecutionException {
        configuration.load();

        List<Path> dataFiles = Files.list(Paths.get(folderPath))
                .filter(path -> path.getFileName().toString().startsWith("dataFile") && path.toString().endsWith(".tsv"))
                .collect(Collectors.toList());

        if (dataFiles.isEmpty()) {
            System.err.println("No data files found in the specified folder.");
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

            executorService.shutdown();
        }

        // Generate statistics
        generateStatistics();
    }

    private void generateStatistics() {
        System.out.println("Files processed successfully: " + statistics.getProcessedFiles());
        System.out.println("Files failed: " + statistics.getFailedFiles());
        System.out.println("Total rows: " + statistics.getTotalRows());
        System.out.println("Processed rows: " + statistics.getProcessedRows());
    }
}