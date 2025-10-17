package com.example.audit_sync.repository.jpa;

import com.example.audit_sync.model.AuditTrail;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.List;

public interface AuditTrailRepository extends JpaRepository<AuditTrail, Long> {

    @Query("SELECT MIN(a.changedAt) FROM AuditTrail a")
    LocalDateTime findOldestChangedAt();

    List<AuditTrail> findByChangedAtBetween(LocalDateTime start, LocalDateTime end);

    List<AuditTrail> findByChangedAtBefore(LocalDateTime cutoffDate);

    @Query(value = "SELECT * FROM audit_trail WHERE id > :lastId ORDER BY id ASC LIMIT :limit", nativeQuery = true)
    List<AuditTrail> findNextBatch(@Param("lastId") Long lastId, @Param("limit") int limit);


}
