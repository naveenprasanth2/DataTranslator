package com.symphony;

public class Statistics {
    private int processedFiles = 0;
    private int failedFiles = 0;
    private int totalRows = 0;
    private int processedRows = 0;

    public synchronized void incrementProcessedFiles() {
        processedFiles++;
    }

    public synchronized void incrementFailedFiles() {
        failedFiles++;
    }

    public synchronized void incrementTotalRows() {
        totalRows++;
    }

    public synchronized void incrementProcessedRows() {
        processedRows++;
    }

    public synchronized int getProcessedFiles() {
        return processedFiles;
    }

    public synchronized int getFailedFiles() {
        return failedFiles;
    }

    public synchronized int getTotalRows() {
        return totalRows;
    }

    public synchronized int getProcessedRows() {
        return processedRows;
    }
}