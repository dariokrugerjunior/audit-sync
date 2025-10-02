package com.example.audit_sync.service;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.example.audit_sync.model.AuditTrail;
import com.example.audit_sync.repository.jpa.AuditTrailRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

@Service
public class AuditTrailService {

    private final AuditTrailRepository repository;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final BlobServiceClient blobServiceClient;

    public AuditTrailService(
            AuditTrailRepository repository,
            @Value("${azure.storage.connection-string}") String connectionString) {

        this.repository = repository;
        this.blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(connectionString)
                .buildClient();
    }

    //CREATE INDEX CONCURRENTLY idx_audit_trail_action_type ON audit_trail (action_type);
    //CREATE INDEX CONCURRENTLY idx_audit_trail_changed_at ON audit_trail (changed_at);

    @Transactional
    public void syncAndPurgeOldData() {
        LocalDateTime cutoffDate = LocalDateTime.now().minusDays(15);
        List<AuditTrail> recordsToArchive = repository.findByChangedAtBefore(cutoffDate);

        if (recordsToArchive.isEmpty()) {
            System.out.println("Sem dados antigos para processar");
            return;
        }

        StringBuilder csvBuilder = new StringBuilder();
        csvBuilder.append("id;action_type;table_name;changed_at;old_data;new_data\n");

        for (AuditTrail audit : recordsToArchive) {
            csvBuilder.append(audit.getId()).append(";");
            csvBuilder.append(escapeCsv(audit.getActionType())).append(";");
            csvBuilder.append(escapeCsv(audit.getTableName())).append(";");
            csvBuilder.append(audit.getChangedAt()).append(";");
            csvBuilder.append(escapeCsv(audit.getOldData())).append(";");
            csvBuilder.append(escapeCsv(audit.getNewData())).append("\n");
        }

        byte[] csvBytes = csvBuilder.toString().getBytes(StandardCharsets.UTF_8);

        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient("audit-trail-old");
        if (!containerClient.exists()) {
            containerClient.create();
        }

        LocalDate cutoffLocalDate = cutoffDate.toLocalDate();
        String folderName = String.format("%02d-%d", cutoffLocalDate.getMonthValue(), cutoffLocalDate.getYear());
        String safeFolderName = folderName.replaceAll("[^a-zA-Z0-9-]", "");
        String blobName = safeFolderName + "/audit_data.csv";

        BlockBlobClient blobClient = containerClient.getBlobClient(blobName).getBlockBlobClient();
        ByteArrayInputStream dataStream = new ByteArrayInputStream(csvBytes);
        blobClient.upload(dataStream, csvBytes.length, true);

        repository.deleteAll(recordsToArchive);

        System.out.println("Arquivamento de dados até " + cutoffDate + " concluído e dados removidos.");
    }

    private String escapeCsv(String value) {
        if (value == null) return "";
        String escaped = value.replace("\"", "\"\"");
        if (escaped.contains(",") || escaped.contains("\"") || escaped.contains("\n")) {
            return "\"" + escaped + "\"";
        }
        return escaped;
    }
}
