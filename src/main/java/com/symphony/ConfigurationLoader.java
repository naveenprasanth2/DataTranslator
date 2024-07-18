package com.symphony;

import java.io.*;
import java.nio.file.*;
import java.util.*;

public class ConfigurationLoader {
    private final String folderPath;
    private final Map<String, String> columnMapping;
    private final Map<String, String> identifierMapping;

    public ConfigurationLoader(String folderPath) {
        this.folderPath = folderPath;
        columnMapping = new HashMap<>();
        identifierMapping = new HashMap<>();
    }

    public void load() throws IOException {
        loadColumnMapping();
        loadIdentifierMapping();
    }

    private void loadColumnMapping() throws IOException {
        Path columnMappingFile = Paths.get(folderPath, "column_mapping.tsv");
        FileUtils.checkFileExists(columnMappingFile);

        try (BufferedReader reader = Files.newBufferedReader(columnMappingFile)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length == 2) {
                    columnMapping.put(parts[0], parts[1]);
                }
            }
        }
    }

    private void loadIdentifierMapping() throws IOException {
        Path identifierMappingFile = Paths.get(folderPath, "identifier_mapping.tsv");
        FileUtils.checkFileExists(identifierMappingFile);

        try (BufferedReader reader = Files.newBufferedReader(identifierMappingFile)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length == 2) {
                    identifierMapping.put(parts[0], parts[1]);
                }
            }
        }
    }

    public Map<String, String> getColumnMapping() {
        return columnMapping;
    }

    public Map<String, String> getIdentifierMapping() {
        return identifierMapping;
    }
}