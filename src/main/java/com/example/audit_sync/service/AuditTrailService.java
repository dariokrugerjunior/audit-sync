package com.example.audit_sync.service;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.example.audit_sync.model.AuditTrail;
import com.example.audit_sync.repository.jpa.AuditTrailRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class AuditTrailService {

    private final AuditTrailRepository repository;
    private final BlobServiceClient blobServiceClient;
    private static final Logger logger = LoggerFactory.getLogger(AuditTrailService.class);

    private static final int BATCH_SIZE = 50000;
    private static final int DAYS_INTERVAL = 15;

    public AuditTrailService(
            AuditTrailRepository repository,
            @Value("${azure.storage.connection-string}") String connectionString) {

        this.repository = repository;
        this.blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(connectionString)
                .buildClient();
    }

    @Async
    @Transactional
    public CompletableFuture<Void> syncAndPurgeOldDataBatch() {
        long lastProcessedId = 0L;
        boolean hasMore = true;
        LocalDate maxDateAllowed = null;

        while (hasMore) {
            List<AuditTrail> batch = repository.findNextBatch(lastProcessedId, BATCH_SIZE);

            if (batch.isEmpty()) {
                hasMore = false;
                break;
            }

            LocalDate minDate = batch.stream()
                    .map(a -> a.getChangedAt().toLocalDate())
                    .min(LocalDate::compareTo)
                    .orElseThrow();

            if (maxDateAllowed == null) {
                maxDateAllowed = minDate.plusDays(DAYS_INTERVAL);
            }

            LocalDate finalMaxDateAllowed = maxDateAllowed;
            List<AuditTrail> batch15Days = batch.stream()
                    .filter(a -> {
                        LocalDate date = a.getChangedAt().toLocalDate();
                        return !date.isBefore(minDate) && !date.isAfter(finalMaxDateAllowed);
                    })
                    .toList();

            if (!batch15Days.isEmpty()) {
                saveAndDeleteBatch(batch15Days, minDate);
            }

            // Se o maior registro no lote ultrapassa o maxDateAllowed, parar
            LocalDate maxDateInBatch = batch.stream()
                    .map(a -> a.getChangedAt().toLocalDate())
                    .max(LocalDate::compareTo)
                    .orElse(minDate);

            if (maxDateInBatch.isAfter(maxDateAllowed)) {
                hasMore = false;
            }

            // Atualizar lastProcessedId com base no último registro do batch15Days (não do batch total)
            if (!batch15Days.isEmpty()) {
                lastProcessedId = batch15Days.get(batch15Days.size() - 1).getId();
            } else {
                // Caso batch15Days vazio, para evitar loop infinito, avançar com o lastProcessedId do batch total
                lastProcessedId = batch.get(batch.size() - 1).getId();
            }
        }

        return CompletableFuture.completedFuture(null);
    }

    private void saveAndDeleteBatch(List<AuditTrail> batch, LocalDate minDate) {
        StringBuilder csvBuilder = new StringBuilder();
        csvBuilder.append("id;action_type;table_name;changed_at;old_data;new_data\n");

        for (AuditTrail audit : batch) {
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

        String folderName = String.format("%02d-%d", minDate.getMonthValue(), minDate.getYear());
        int nextSequence = getNextSequenceNumber(containerClient, folderName);
        String blobName = folderName + "/audit_trail_" + nextSequence + ".csv";

        BlockBlobClient blobClient = containerClient.getBlobClient(blobName).getBlockBlobClient();
        ByteArrayInputStream dataStream = new ByteArrayInputStream(csvBytes);
        blobClient.upload(dataStream, csvBytes.length, true);

        repository.deleteAll(batch);
        logger.info("Batch arquivado e removido, inicio do período: {}", minDate);
    }

    private int getNextSequenceNumber(BlobContainerClient containerClient, String folderName) {
        int maxSequence = 0;

        PagedIterable<BlobItem> blobs = containerClient.listBlobs(new ListBlobsOptions().setPrefix(folderName + "/"), null);

        for (BlobItem blobItem : blobs) {
            String blobName = blobItem.getName(); // ex: folderName/audit_trail_1.csv

            Pattern pattern = Pattern.compile("audit_trail_(\\d+)\\.csv$");
            Matcher matcher = pattern.matcher(blobName);

            if (matcher.find()) {
                int seq = Integer.parseInt(matcher.group(1));
                if (seq > maxSequence) {
                    maxSequence = seq;
                }
            }
        }
        return maxSequence + 1;
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
