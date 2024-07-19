package com.symphony;

import lombok.Getter;

import java.io.*;
import java.nio.file.*;
import java.util.*;

@Getter
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

    //helps in doing the column mapping
    private void loadColumnMapping() throws IOException {
        Path columnMappingFile = Paths.get(folderPath, "column_mapping.tsv");
        checkExists(columnMappingFile, columnMapping);
    }

    //helps in doing the identifier mapping
    private void loadIdentifierMapping() throws IOException {
        Path identifierMappingFile = Paths.get(folderPath, "identifier_mapping.tsv");
        checkExists(identifierMappingFile, identifierMapping);
    }

    private void checkExists(Path identifierMappingFile, Map<String, String> identifierMapping) throws IOException {
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
}