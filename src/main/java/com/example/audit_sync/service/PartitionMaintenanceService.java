package com.example.audit_sync.service;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.example.audit_sync.repository.PartitionMaintenanceRepository;
import com.example.audit_sync.utils.CsvExportUtil;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class PartitionMaintenanceService {

    private final PartitionMaintenanceRepository partitionRepository;
    private final BlobServiceClient blobServiceClient;
    private static final Logger logger = LoggerFactory.getLogger(PartitionMaintenanceService.class);

    public PartitionMaintenanceService(
            PartitionMaintenanceRepository partitionRepository,
            @Value("${azure.storage.connection-string}") String connectionString) {
        this.partitionRepository = partitionRepository;
        this.blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(connectionString)
                .buildClient();
    }

    @Async
    @Transactional
    public CompletableFuture<Void> archiveAndDropOldPartitions() throws SQLException {
        List<String> partitions = partitionRepository.listPartitions();
        String currentPartition = findPartitionForToday(partitions);
        if (currentPartition == null) {
            logger.info("Nenhuma partição corresponde à data atual.");
            return CompletableFuture.completedFuture(null);
        }

        logger.info("Partição atual identificada: {}", currentPartition);

        for (String partition : partitions) {
            if (partition.equalsIgnoreCase(currentPartition)) {
                logger.info("Chegou na partição atual, parando processamento.");
                break;
            }
            logger.info("Processando partição: {}", partition);
            if (partitionRepository.isPartitionEmpty(partition)) {
                logger.info("Partição {} está vazia, apenas removendo.", partition);
            } else {
                try (ResultSet rs = partitionRepository.getPartitionData(partition)) {
                    String csvData = CsvExportUtil.resultSetToCsv(rs);
                    uploadCsvToBlob(partition, csvData);
                    logger.info("Partição {} arquivada.", partition);
                }
            }
            partitionRepository.dropPartition(partition);
            logger.info("Partição {} removida.", partition);
        }
        return CompletableFuture.completedFuture(null);
    }

    private String findPartitionForToday(List<String> partitions) {
        LocalDate today = LocalDate.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");

        for (String partition : partitions) {
            // Exemplo de nome: audit_trail_partitioned_20251001_20251015
            String[] parts = partition.split("_");
            String startStr = parts[parts.length - 2];
            String endStr = parts[parts.length - 1];
            LocalDate startDate = LocalDate.parse(startStr, formatter);
            LocalDate endDate = LocalDate.parse(endStr, formatter);

            if ((today.isEqual(startDate) || today.isAfter(startDate)) &&
                    (today.isEqual(endDate) || today.isBefore(endDate))) {
                return partition;
            }
        }
        return null;
    }

    private void uploadCsvToBlob(String partitionName, String csvData) {
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient("audit-trail");
        if (!containerClient.exists()) {
            containerClient.create();
        }
        String blobName = partitionName + "/audit_data.csv";
        BlockBlobClient blobClient = containerClient.getBlobClient(blobName).getBlockBlobClient();

        ByteArrayInputStream dataStream = new ByteArrayInputStream(csvData.getBytes(StandardCharsets.UTF_8));
        blobClient.upload(dataStream, csvData.length(), true);
    }
}