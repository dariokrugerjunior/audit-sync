package com.example.audit_sync.utils;

import java.sql.ResultSet;
import java.sql.SQLException;

public class CsvExportUtil {
    public static String resultSetToCsv(ResultSet rs) throws SQLException {
        StringBuilder csvBuilder = new StringBuilder();
        csvBuilder.append("id;action_type;table_name;changed_at;old_data;new_data\n");
        while (rs.next()) {
            csvBuilder.append(rs.getLong("id")).append(";");
            csvBuilder.append("\"").append(rs.getString("action_type").replace("\"", "\"\"")).append("\";");
            csvBuilder.append("\"").append(rs.getString("table_name").replace("\"", "\"\"")).append("\";");
            csvBuilder.append(rs.getTimestamp("changed_at")).append(";");
            csvBuilder.append("\"").append(rs.getString("old_data").replace("\"", "\"\"")).append("\";");
            csvBuilder.append("\"").append(rs.getString("new_data").replace("\"", "\"\"")).append("\"\n");
        }
        return csvBuilder.toString();
    }
}

