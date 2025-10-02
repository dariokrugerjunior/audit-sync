package com.example.audit_sync.repository;

import com.example.audit_sync.repository.interfaces.AuditTrailRepositoryCustom;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.transaction.Transactional;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class AuditTrailRepositoryCustomImpl implements AuditTrailRepositoryCustom {

    @PersistenceContext
    private EntityManager em;

    @Override
    public List<String> findPartitions() {
        String sql = "SELECT inhrelid::regclass::text AS partition_name FROM pg_inherits WHERE inhparent = 'public.audit_trail_partitioned'::regclass";
        return em.createNativeQuery(sql).getResultList();
    }

    @Override
    @Transactional
    public void dropPartition(String partitionName) {
        String sql = "DROP TABLE IF EXISTS " + partitionName;
        em.createNativeQuery(sql).executeUpdate();
    }
}