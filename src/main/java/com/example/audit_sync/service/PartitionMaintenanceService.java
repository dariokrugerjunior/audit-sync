package com.example.audit_sync.service;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Service
public class PartitionMaintenanceService {

    private final DataSource dataSource;
    private final BlobServiceClient blobServiceClient;

    public PartitionMaintenanceService(DataSource dataSource,
                                       @Value("${azure.storage.connection-string}") String connectionString) {
        this.dataSource = dataSource;
        this.blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(connectionString)
                .buildClient();
    }

    private String getCurrentPartition() throws SQLException {
        String sql = "SELECT inhrelid::regclass::text AS partition_name " +
                "FROM pg_inherits " +
                "WHERE inhparent = 'public.audit_trail_partitioned'::regclass " +
                "ORDER BY partition_name DESC LIMIT 1";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString("partition_name");
            }
        }
        return null;
    }

    private List<String> listPartitions() throws SQLException {
        List<String> partitions = new ArrayList<>();
        String sql = "SELECT inhrelid::regclass::text AS partition_name FROM pg_inherits " +
                "WHERE inhparent = 'public.audit_trail_partitioned'::regclass;";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                partitions.add(rs.getString("partition_name"));
            }
        }
        return partitions;
    }

    @Transactional
    public void archiveAndDropOldPartitions() throws SQLException {
        String currentPartition = getCurrentPartition();
        if (currentPartition == null) {
            System.out.println("Nenhuma partição encontrada.");
            return;
        }
        System.out.println("Partição atual identificada: " + currentPartition);

        List<String> partitions = listPartitions();

        for (String partition : partitions) {
            if (!partition.equalsIgnoreCase(currentPartition)) {
                System.out.println("Processando partição: " + partition);
                String csvData = exportPartitionToCsv(partition);
                uploadCsvToBlob(partition, csvData);
                dropPartition(partition);
                System.out.println("Partição " + partition + " arquivada e removida.");
            }
        }
    }

    private String exportPartitionToCsv(String partition) throws SQLException {
        StringBuilder csvBuilder = new StringBuilder();
        String sql = "SELECT id, action_type, table_name, changed_at, old_data, new_data FROM " + partition;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            csvBuilder.append("id,action_type,table_name,changed_at,old_data,new_data\n");

            while (rs.next()) {
                csvBuilder.append(rs.getLong("id")).append(",");
                csvBuilder.append("\"").append(rs.getString("action_type").replace("\"", "\"\"")).append("\",");
                csvBuilder.append("\"").append(rs.getString("table_name").replace("\"", "\"\"")).append("\",");
                csvBuilder.append(rs.getTimestamp("changed_at")).append(",");
                csvBuilder.append("\"").append(rs.getString("old_data").replace("\"", "\"\"")).append("\",");
                csvBuilder.append("\"").append(rs.getString("new_data").replace("\"", "\"\"")).append("\"\n");
            }
        }
        return csvBuilder.toString();
    }

    private void uploadCsvToBlob(String partitionName, String csvData) {
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient("audit_trail");
        if (!containerClient.exists()) {
            containerClient.create();
        }
        String blobName = partitionName + "/audit_data.csv";
        BlockBlobClient blobClient = containerClient.getBlobClient(blobName).getBlockBlobClient();

        ByteArrayInputStream dataStream = new ByteArrayInputStream(csvData.getBytes(StandardCharsets.UTF_8));
        blobClient.upload(dataStream, csvData.length(), true);
    }

    private void dropPartition(String partitionName) throws SQLException {
        String sql = "DROP TABLE IF EXISTS " + partitionName;
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }
}