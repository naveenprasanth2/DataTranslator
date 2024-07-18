package com.symphony;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class DataFileProcessor implements Callable<Void> {
    private static final Logger log = LoggerFactory.getLogger(DataFileProcessor.class);
    private final Path dataFile;
    private final ConfigurationLoader configuration;
    private final Statistics statistics;

    public DataFileProcessor(Path dataFile, ConfigurationLoader configuration, Statistics statistics) {
        this.dataFile = dataFile;
        this.configuration = configuration;
        this.statistics = statistics;
    }

    @Override
    public Void call() {
        try (BufferedReader reader = Files.newBufferedReader(dataFile)) {
            String headerLine = reader.readLine();
            if (headerLine == null) {
                throw new IOException("Empty data file: " + dataFile.getFileName());
            }

            List<String> headers = Arrays.asList(headerLine.split("\t"));
            Map<String, Integer> headerIndexMap = new HashMap<>();
            for (int i = 0; i < headers.size(); i++) {
                headerIndexMap.put(headers.get(i), i);
            }

            List<Integer> columnsToExtract = configuration.getColumnMapping().keySet().stream()
                    .map(headerIndexMap::get)
                    .filter(Objects::nonNull)
                    .toList();

            Path outputFile = Paths.get(dataFile.getParent().toString(), "output" + dataFile.getFileName().toString().replace("dataFile", ""));
            try (BufferedWriter writer = Files.newBufferedWriter(outputFile)) {
                writeOutputHeader(writer);

                String line;
                while ((line = reader.readLine()) != null) {
                    List<String> values = Arrays.asList(line.split("\t"));
                    String identifier = values.getFirst();
                    if (configuration.getIdentifierMapping().containsKey(identifier)) {
                        writeOutputRow(writer, values, columnsToExtract);
                        statistics.incrementProcessedRows();
                    }
                    statistics.incrementTotalRows();
                }
            }
        } catch (IOException e) {
            log.error("Failed to process file: {} - {}", dataFile.getFileName(), e.getMessage());
            statistics.incrementFailedFiles();
        }
        return null;
    }

    private void writeOutputHeader(BufferedWriter writer) throws IOException {
        List<String> outputHeaders = new ArrayList<>();
        configuration.getColumnMapping().forEach((original, translated) -> outputHeaders.add(translated));
        writer.write(String.join("\t", outputHeaders));
        writer.newLine();
    }

    private void writeOutputRow(BufferedWriter writer, List<String> values, List<Integer> columnsToExtract) throws IOException {
        List<String> outputValues = columnsToExtract.stream().map(values::get).toList();
        writer.write(String.join("\t", outputValues));
        writer.newLine();
    }
}