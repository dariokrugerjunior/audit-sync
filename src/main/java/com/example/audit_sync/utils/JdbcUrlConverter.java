package com.example.audit_sync.utils;

public class JdbcUrlConverter {

    public static String convertToJdbc(String url) {
        if (url == null || !url.startsWith("postgresql://")) {
            throw new IllegalArgumentException("URL inválida. Esperado formato: postgresql://user:password@host:port/database");
        }

        try {
            // Remove o prefixo
            String raw = url.substring("postgresql://".length());

            // Divide entre credenciais e host
            String[] parts = raw.split("@");
            if (parts.length != 2) {
                throw new IllegalArgumentException("URL malformada: faltando '@' separando usuário e host.");
            }

            // Separa usuário e senha
            String[] userPass = parts[0].split(":", 2);
            String user = userPass[0];
            String password = userPass[1];

            // Parte do host + database
            String jdbcPart = parts[1];

            return String.format("jdbc:postgresql://%s?user=%s&password=%s", jdbcPart, user, password);
        } catch (Exception e) {
            throw new RuntimeException("Erro ao converter a URL para formato JDBC: " + e.getMessage(), e);
        }
    }
}