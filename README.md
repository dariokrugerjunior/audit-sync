# Audit Sync

Este projeto realiza a sincronização, arquivamento e manutenção de trilhas de auditoria (audit_trail) em banco de dados PostgreSQL, exportando dados antigos para arquivos CSV e armazenando-os em blob storage Azure. Também gerencia partições do banco para otimizar performance e armazenamento.

## Pré-requisitos
- Java 17+
- Maven
- Banco de dados PostgreSQL
- Azure Blob Storage

## Configuração
Antes de executar o projeto, é necessário configurar as variáveis de ambiente no sistema ou no arquivo de configuração (application.yml). As principais variáveis são:

```
DB_URL_ADMIN=postgresql://vvvl_dev_admin:W9wCJJYPxOyRDzkr0Rvr@pgsqlf-vvvl-brsouth-001-dev.postgres.database.azure.com:5432/vvvl_dev_admin
AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net
```

Adicione outras variáveis conforme necessário para o ambiente de banco e storage.

## Execução
1. Instale as dependências:
   ```
   mvn clean install
   ```
2. Execute a aplicação:
   ```
   mvn spring-boot:run
   ```

## Funcionalidades
- Sincronização e arquivamento de dados antigos de audit_trail.
- Exportação para CSV e upload para Azure Blob Storage.
- Manutenção e remoção de partições antigas do banco.
- Operações paralelas e logs estruturados via SLF4J.

## Observações
- Certifique-se de que as variáveis de ambiente estejam corretamente configuradas antes de iniciar o sistema.
- Consulte o arquivo `application.yml` para detalhes adicionais de configuração.

---

Para dúvidas ou sugestões, entre em contato com o time de desenvolvimento.

