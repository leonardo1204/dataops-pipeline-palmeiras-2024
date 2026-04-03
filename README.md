DataOps Pipeline: Palmeiras - Brasileirao 2024 📊

Este projeto estabelece um pipeline de dados End-to-End para extração, tratamento e análise da performance do Palmeiras no Campeonato Brasileiro de 2024. O objetivo foi transformar dados brutos de uma API externa em uma estrutura de Data Lake consultável via SQL na nuvem.

Arquitetura e Fluxo
O pipeline segue o modelo de arquitetura de medalhão (Medallion Architecture) utilizando serviços serverless da AWS:

Ingestão (Bronze): Coleta de dados via API-Football usando Python (requests). Os dados são salvos em formato JSON bruto no Amazon S3 para garantir a imutabilidade da fonte.

Processamento (Silver): Utilização da biblioteca Pandas para limpeza e normalização. Foi aplicada uma lógica de negócio para rotular os resultados (Vitoria, Empate ou Derrota) baseada na condição de mandante ou visitante do clube.

Entrega (Analytics): Conversão dos dados para NDJSON para otimização de leitura no Amazon Athena, permitindo consultas analíticas rápidas sem a necessidade de um servidor de banco de dados relacional (RDS) ativo.

Tecnologias Utilizadas
Linguagem: Python 3.x (Boto3, Pandas, Requests)

Infraestrutura: AWS S3 (Storage) e Amazon Athena (Query Engine)

Versionamento: Git/GitHub (com foco em Security Scanning para proteção de chaves IAM)

Desafios Tecnicos Superados
Logica de Negocio: Refatoração do script para tratar corretamente resultados de jogos fora de casa, garantindo a integridade dos rótulos de dados.

Data Serialization: Implementação de formato JSON Lines (NDJSON) para evitar erros de parsing e melhorar a performance de consultas no Athena.

Seguranca (DevSecOps): Configuração de proteção de segredos no repositório para evitar o vazamento de credenciais de infraestrutura.

Exemplo de Consulta (SQL)
Com o pipeline finalizado, é possível realizar análises complexas diretamente no Athena:

SQL
-- Analisar desempenho contra adversarios especificos
SELECT data, adversario, placar, resultado 
FROM "seu_banco"."palmeiras_silver"
WHERE adversario = 'Corinthians'
ORDER BY data;

Leonardo Freitas
Estudante de Analise e Desenvolvimento de Sistemas
