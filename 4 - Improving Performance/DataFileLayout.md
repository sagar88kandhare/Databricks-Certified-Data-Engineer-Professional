**Data File Layout: Melhores Práticas no Databricks**

- Organização eficiente e estrutura de armazenamento de arquivos são essenciais para performance e escalabilidade.

## Estratégias de Otimização

### Partitioning
**O que é:** Técnica que divide fisicamente os dados em subpastas baseadas nos valores de uma ou mais colunas, permitindo que o Spark leia apenas as partições relevantes durante as consultas (partition pruning).

**Como ativar:**
```sql
-- Criar tabela com particionamento
CREATE TABLE vendas (
  id INT,
  produto STRING,
  valor DECIMAL(10,2),
  data DATE
)
PARTITIONED BY (data);

-- Ou ao escrever dados
df.write.partitionBy("data").saveAsTable("vendas");
```

**Boas práticas:**
- Particionamento melhora o desempenho em consultas de grandes volumes de dados (normalmente, tabelas acima de 1 TB).
- Use partitioning para colunas de baixa cardinalidade e comumente filtradas nas queries.
- Evite particionar por colunas de alta cardinalidade, pois isso gera muitas partições pequenas e prejudica a performance.
- O número ideal de arquivos por partição está entre 100 e 1000.
- Atenção: Mudanças nos requisitos de negócio podem exigir reescrita das partições.
- Avalie periodicamente o layout das partições para evitar file skew (distribuição desigual de dados entre partições).

### Z-Order Indexing
**O que é:** Técnica de co-localização que organiza dados relacionados próximos fisicamente nos arquivos, otimizando consultas com múltiplos filtros sem criar subpastas adicionais.

**Como ativar:**
```sql
-- Executar Z-Order em colunas frequentemente filtradas
OPTIMIZE vendas
ZORDER BY (produto, regiao);
```

**Boas práticas:**
- Z-Order agrupa dados otimizando o acesso sem criar subpastas.
- Ideal para consultas seletivas em colunas específicas ou múltiplas colunas.
- Não substitui o particionamento, mas pode ser usado em conjunto para performance máxima.
- Não suporta operações incrementais: execute o comando após a chegada de novos dados.
- Use Z-Order especialmente em tabelas onde filtros multi-coluna são frequentes.

### Liquid Clustering
**O que é:** Evolução do clustering que oferece flexibilidade para alterar as colunas de clustering sem reescrever a tabela, com otimização incremental automática.

**Como ativar:**
```sql
-- Criar tabela com Liquid Clustering
CREATE TABLE vendas (
  id INT,
  produto STRING,
  regiao STRING,
  data DATE
)
CLUSTER BY (produto, regiao);

-- Alterar colunas de clustering
ALTER TABLE vendas CLUSTER BY (regiao, data);
```

**Boas práticas:**
- Versão aprimorada do clustering, oferece flexibilidade e melhor performance.
- Permite reorganização eficiente dos dados sem necessidade de reescrita total.
- Útil para ambientes dinâmicos e grandes volumes, reduz impactos de mudanças nos requisitos de negócio.

### Automatic Clustering (Auto Clustering)
**O que é:** Recurso que aplica clustering automaticamente durante operações de escrita em tabelas com Liquid Clustering habilitado, sem necessidade de comandos manuais de otimização.

**Como ativar:**
```sql
-- Habilitar Auto Clustering na tabela
ALTER TABLE vendas SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);
```

**Boas práticas:**
- Automatic Clustering reorganiza automaticamente os dados em tabelas Delta, ajustando o layout físico de acordo com padrões de uso e consultas, sem necessidade de comandos manuais.
- Beneficia joins, filtros frequentes e reduz file skew, sendo ideal para cargas de dados dinâmicas e operações contínuas.
- Facilita a manutenção, pois a otimização ocorre automaticamente, dispensando tarefas periódicas de otimização.
- Pode ser usado em conjunto com Z-Order e particionamento para performance máxima.
- Limitado a tabelas Delta em ambientes gerenciados Databricks; recomenda-se monitorar periodicamente os ganhos de performance.

### Predictive Optimization
**O que é:** Recurso avançado que usa machine learning para analisar padrões de uso e aplicar automaticamente técnicas de otimização (compactação, clustering, vacuum) de forma proativa.

**Como ativar:**
```sql
-- Habilitar Predictive Optimization para uma tabela
ALTER TABLE vendas SET TBLPROPERTIES (
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.targetFileSize' = '128MB'
);

-- Habilitar para todo o schema
ALTER SCHEMA meu_schema ENABLE PREDICTIVE OPTIMIZATION;

-- Verificar status
DESCRIBE DETAIL vendas;
```

**Boas práticas:**
- Predictive Optimization utiliza inteligência artificial e machine learning para analisar o histórico de consultas e cargas, reorganizando pró-ativamente o layout físico dos arquivos.
- Antecipando os padrões de uso, consegue otimizar o armazenamento antes mesmo que as consultas sejam feitas, com benefícios em ambientes de grande volume e cargas dinâmicas.
- É recomendada para cenários com consultas complexas, múltiplos filtros e operações concorrentes.
- Atua de forma complementar ao clustering e ao particionamento, sendo ideal para maximizar a performance com mínima intervenção manual.

### Deletion Vectors (Vetores de Exclusão)
**O que é:** Tecnologia do Delta Lake/Databricks usada para marcar linhas como excluídas de forma eficiente, sem reescrever todo o arquivo de dados. As linhas são sinalizadas via um vetor de exclusão, acelerando operações de DELETE, UPDATE e MERGE.

**Como funciona:**
- Ao invés de remover fisicamente os registros dos arquivos Parquet, o Delta Lake mantém um vetor de exclusão indicando quais linhas devem ser ignoradas durante a leitura.
- Isso permite consultas rápidas, pois os dados marcados não são retornados, e elimina o custo de reescrita de arquivos inteiros.

**Vantagens:**
- Opera com arquivos grandes de forma eficiente, ideal para ambientes com volumes elevados e operações frequentes de atualização/exclusão.
- Reduz o tempo e recursos de operações DELETE/UPDATE/MERGE.
- Permite operações concorrentes e manutenção simplificada.

**Limitações:**
- Os dados "excluídos" ainda permanecem fisicamente, podendo aumentar o uso de armazenamento ao longo do tempo.
- Recomenda-se executar VACUUM periodicamente para remover fisicamente os registros.
- Disponível apenas em tabelas Delta Lake gerenciadas e ambientes Databricks com suporte à feature.

**Boas práticas:**
- Ideal para cargas dinâmicas, casos de streaming e tabelas de auditoria.
- Combine o uso de Vetores de Exclusão com monitoramento do espaço e operações VACUUM para garantir eficiência.
- Consulte a documentação Databricks para detalhes de ativação, pois a feature pode variar conforme a versão e configuração do ambiente.

**Exemplo:**
- Ao executar um comando DELETE, o Delta Lake pode marcar as linhas como excluídas usando deletion vector, evitando a reescrita do arquivo:
```sql
DELETE FROM vendas WHERE produto = 'XYZ';
```
- Após diversas exclusões, recomenda-se:
```sql
VACUUM vendas RETAIN 168 HOURS;  -- Remove arquivos obsoletos após 7 dias
```

- Para mais detalhes consulte: https://docs.databricks.com/en/delta/deletion-vectors.html

## Recomendações Gerais
- Utilize partitioning apenas em tabelas grandes, filtradas frequentemente, por colunas apropriadas.
- Combine Z-Order com particionamento para consultas complexas em múltiplas colunas.
- Considere Liquid Clustering, Auto Clustering ou Predictive Optimization para ambientes dinâmicos e com grande volume de dados.
- Monitore o tamanho dos arquivos; evite arquivos pequenos para maximizar throughput.
- Verifique periodicamente a distribuição dos dados para evitar file skew e garantir alta performance.