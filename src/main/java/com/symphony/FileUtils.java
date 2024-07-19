package com.symphony;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.*;

public class FileUtils {
    private FileUtils(){}
    public static void checkFileExists(Path path) throws IOException {
        if (!Files.exists(path)) {
            throw new FileNotFoundException("File not found: " + path.getFileName());
        }
    }
}