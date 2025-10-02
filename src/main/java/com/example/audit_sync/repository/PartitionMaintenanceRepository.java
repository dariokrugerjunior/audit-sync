package com.example.audit_sync.repository;

import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Repository
public class PartitionMaintenanceRepository {
    private final DataSource dataSource;

    public PartitionMaintenanceRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public List<String> listPartitions() throws SQLException {
        List<String> partitions = new ArrayList<>();
        String sql = "SELECT inhrelid::regclass::text AS partition_name FROM pg_inherits WHERE inhparent = 'public.audit_trail_partitioned'::regclass;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                partitions.add(rs.getString("partition_name"));
            }
        }
        return partitions;
    }

    public boolean isPartitionEmpty(String partition) throws SQLException {
        String sql = "SELECT COUNT(1) FROM " + partition;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getInt(1) == 0;
            }
        }
        return true;
    }

    public ResultSet getPartitionData(String partition) throws SQLException {
        String sql = "SELECT id, action_type, table_name, changed_at, old_data, new_data FROM " + partition;
        Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql);
        return ps.executeQuery();
    }

    public void dropPartition(String partitionName) throws SQLException {
        String sql = "DROP TABLE IF EXISTS " + partitionName;
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }
}
