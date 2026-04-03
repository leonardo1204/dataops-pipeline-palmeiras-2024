# DataOps Pipeline - Palmeiras (Brasileirão 2024) 🐷🚀

## 📋 Sobre o Projeto
Este projeto consiste em um pipeline de dados **End-to-End** desenvolvido para extrair, transformar e carregar (ETL) dados históricos da campanha do Palmeiras no Campeonato Brasileiro de 2024. 

A arquitetura foi desenhada utilizando o conceito de **Data Lake Moderno** na AWS, garantindo escalabilidade, segurança e baixo custo operacional através de serviços Serverless.

## 🛠️ Tecnologias e Habilidades
* **Python 3.x**: Linguagem principal para orquestração e lógica de ETL.
* **Pandas**: Biblioteca utilizada para limpeza e transformação de dados (Data Wrangling).
* **Boto3**: SDK oficial da AWS para integração do Python com serviços de nuvem.
* **AWS S3**: Armazenamento de objetos utilizando a **Medallion Architecture** (Camadas Bronze e Silver).
* **AWS Athena**: Motor de consulta SQL utilizado para análise de dados diretamente no S3 (Schema-on-Read).
* **API-Football**: Fonte de dados externa consumida via requisições REST.

## 🏗️ Arquitetura do Pipeline
1.  **Ingestão (Bronze):** O script consome a API REST, autentica via API Key e persiste os dados brutos (Raw JSON) no S3.
2.  **Transformação (Silver):** Utilizando Pandas, os dados são limpos, tipados e convertidos para o formato **NDJSON (JSON Lines)**. Este passo é crucial para garantir a performance de leitura do Athena e ferramentas de Big Data.
3.  **Análise:** Criação de tabelas via SQL no Amazon Athena, permitindo consultas rápidas sem a necessidade de manter um banco de dados relacional tradicional ligado.



## 🚀 Desafios Superados
* **API Throttling & Constraints**: Adaptação da estratégia de ingestão para respeitar as limitações de endpoints e planos de subscrição (Free tier).
* **Data Serialization**: Resolução de erros de parsing no Athena através da implementação de serialização linha a linha (NDJSON), evitando problemas com delimitadores de listas JSON padrão.
* **Cloud Security**: Implementação de políticas de acesso programático via IAM (Identity and Access Management).

## 📊 Como Visualizar os Dados
Após a execução do pipeline, os dados podem ser consultados no AWS Athena com o seguinte comando:

```sql
SELECT * FROM "seu_banco"."palmeiras_silver"
ORDER BY data DESC;
