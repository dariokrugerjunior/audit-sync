# Audit Sync

Este projeto é um serviço Java Spring Boot para sincronização e manutenção de trilhas de auditoria.

## Estrutura
- **src/main/java**: Código-fonte principal
- **src/main/resources**: Arquivos de configuração e recursos
- **src/test/java**: Testes automatizados

## Principais componentes
- **model**: Entidades de domínio
- **repository**: Repositórios JPA e customizados
- **service**: Lógica de negócio
- **job**: Jobs agendados (Quartz)
- **config**: Configurações de DataSource e Quartz
- **utils**: Utilitários

## Como executar
1. Instale o Java 17+ e Maven
2. Execute `mvnw.cmd spring-boot:run` na raiz do projeto

## Configuração
Edite o arquivo `src/main/resources/application.yml` conforme necessário para seu ambiente.

## Testes
Execute `mvnw.cmd test` para rodar os testes automatizados.

## Licença
Este projeto é privado e para uso interno.

