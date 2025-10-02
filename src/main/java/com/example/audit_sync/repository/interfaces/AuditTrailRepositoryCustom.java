package com.example.audit_sync.repository.interfaces;

import java.util.List;

public interface AuditTrailRepositoryCustom {
    List<String> findPartitions();

    void dropPartition(String partitionName);
}