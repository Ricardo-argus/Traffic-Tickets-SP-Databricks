#  Multas de Trânsito do Estado de São Paulo — Pipeline Delta Lake Medallion

Pipeline de dados **ponta a ponta** que processa dados públicos de multas de trânsito **pagas** e **vencidas** de todos os municípios do Estado de São Paulo, seguindo a **arquitetura Medallion (Bronze → Silver → Gold)** sobre **Databricks + Delta Lake**, com transformações em **PySpark**, métricas analíticas em **SQL**, camada analítica complementar em **dbt** e qualidade de código garantida por uma suíte de **24 testes pytest**.

| | |
| --- | --- |
| **Volume processado** | ~2,1 milhões de registros brutos (1.350.117 multas pagas + 749.059 vencidas) |
| **Período dos dados** | Novembro/2025 a Agosto/2026 (snapshots mensais) |
| **Tabelas geradas** | 3 Bronze · 4 Silver · 1 Gold + 4 tabelas de métricas + 1 export parquet |
| **Testes** | 24 testes pytest (camadas bronze, silver e gold) + testes dbt |

---

##  Sobre o Projeto

Os dados brutos são arquivos mensais públicos, no formato CSV, contendo a quantidade de multas de trânsito por município, código de infração, UF da placa e tipo/categoria de veículo — em dois conjuntos: **multas pagas** e **multas vencidas** (não pagas). Como referência de domínio, o projeto usa a **tabela RENAINF de códigos de infração** (258 códigos), que traz a descrição e a **gravidade** de cada infração.

O pipeline ingere esses arquivos no Bronze, aplica as regras de negócio no Silver (limpeza, deduplicação de snapshots cumulativos, padronização, geração de chaves e datas) e constrói no Gold as tabelas analíticas e métricas prontas para consumo — incluindo um ranking de **infrações graves por município** exportado em parquet.

Este é um projeto de **portfólio em Data Engineering**, desenvolvido para demonstrar na prática:

- Arquitetura Medallion sobre Delta Lake;
- Engenharia de dados com PySpark e SQL no Databricks;
- Governança de tabelas no Unity Catalog;
- Código testável (funções puras + `main()` como orquestrador) e testes automatizados;
- Camada analítica com dbt e testes de qualidade de dados;
- Boas práticas de organização, logging e configuração centralizada.

---

##  Stack Tecnológica

| Tecnologia | Uso no projeto |
| --- | --- |
| **Databricks** | Plataforma de execução (Spark + Delta Lake + Unity Catalog) |
| **PySpark** | Transformações das camadas Bronze e Silver |
| **SQL** | Métricas e agregações da camada Gold |
| **Delta Lake** | Formato de armazenamento de todas as tabelas (ACID, schema evolution) |
| **dbt** | Camada analítica complementar e testes de qualidade |
| **pytest** | Testes unitários e de regras de negócio |
| **Git / GitHub** | Versionamento |

---

##  Arquitetura Medallion

```text
CSVs mensais (data/)
      │
      ▼
Bronze  — ingestão (src/bronze_pipeline)   → multas_pagas · multas_vencidas · infracoes_codes
      │
      ▼
Silver  — tratamento (src/silver_pipeline) → silver_pagas · silver_vencidas · municipios · gold_multas
      │
      ▼
Gold    — métricas  (src/gold_pipeline)    → 4 tabelas de métricas + parquet de infrações graves
      │
      ▼
dbt     — analítico (dbt/)                  → models silver/gold + testes de qualidade
      │
      ▼
Análise / Dashboards / Consumo
```

###  Bronze — Ingestão

Os três arquivos de origem são lidos e gravados como tabelas Delta com o mínimo de alteração, preservando rastreabilidade:

- **`multas_pagas`** (1.350.117 linhas) — lida com schema explícito (`SCHEMA_PAID`), garantindo tipos corretos;
- **`multas_vencidas`** (749.059 linhas) — leitura com inferência de schema;
- **`infracoes_codes`** (258 linhas) — tabela RENAINF de códigos de infração, com **sanitização dos nomes de colunas** (espaços, parênteses e acentos especiais → `_`) para compatibilidade com Delta.

###  Silver — Tratamento e Regras de Negócio

O coração do pipeline. Principais transformações aplicadas:

1. **Padronização**: as colunas das vencidas são renomeadas ao layout das pagas (`QTDE` → `QUANTIDADE`, `NOME_MUNICIPIO` → `MUNICIPIO`);
2. **Filtro de qualidade**: remoção de registros com `MUNICIPIO` nulo (não informado);
3. **Tratamento de nulos**: `QUANTIDADE` nula conta como 1 multa (fillna nas pagas; coluna `QUANTIDADE_AJUSTADA` nas vencidas);
4. **Deduplicação de snapshots cumulativos**: as vencidas são reportadas em *todos* os meses subsequentes — o pipeline mantém apenas o primeiro registro de cada multa (dedup por chave da multa ordenada por `ANO`/`MES`);
5. **Agregação no grain** (município, infração, UF da placa, tipo/categoria do veículo, mês, ano) somando as quantidades;
6. **Geração de chave**: `ID_MULTA` sequencial e coluna `DATA` (`MES` + `ANO` → data `yyyy-MM`);
7. **Dimensão `municipios`** (645 linhas): união distinta dos municípios das duas fontes;
8. **Enriquecimento**: coluna `STATUS` (`PAGA` / `VENCIDA`) e tabela **`gold_multas`** (386.162 linhas) unificando as duas fontes, com veículos não identificados (`TIPO`/`CATEGORIA` nulos) preenchidos como `'Nao Identificado'`.

Tabelas geradas: `silver_pagas` (241.797), `silver_vencidas` (144.365), `municipios` (645) e `gold_multas` (386.162).

###  Gold — Métricas e Agregações

Quatro tabelas de métricas construídas em SQL sobre a `gold_multas` + um export em parquet:

| Métrica | Regra de negócio |
| --- | --- |
| **`multas_metricas_municipios`** | Total de vencidas por município com classificação de impacto: **ALTO** (> 250), **MEDIO** (> 100), **BAIXA**; municípios sem correspondência agrupados em `'OUTROS'` |
| **`multas_metricas_veiculo`** | Total de vencidas por tipo de veículo |
| **`multas_metricas_gravidade`** | Total por gravidade/mês/ano, via join com a RENAINF (`CONCAT(código, '-', desdobramento) = CODIGO_INFRACAO`); gravidade ausente → `'DESCONHECIDA'` |
| **`quantidade_multas_pagas_por_veiculo`** | Multas pagas por data e tipo de veículo na janela 30/10/2025 – 01/09/2026, classificadas **HIGH QUANTITY** (> 50.000), **MEDIUM QUANTITY** (> 20.000) e **LOW QUANTITY** |
| **Parquet exportado** | Infrações **graves** por município (gravidade com dígito inicial > 5 ou contendo `Gravíss`) → `data/multas_gravidade_per_municipality.parquet` |

>  O join com a RENAINF exige `CONCAT(CAST(Código_da_Infração AS STRING), '-', CAST(Desdob. AS STRING))`, pois a `gold_multas` armazena o código como `5010-0` (string composta).

---

##  Estrutura do Projeto

```text
Traffic-Tickets-SP-Databricks/
├── data/                      # CSVs mensais de origem + parquet exportado
├── dbt/                        # projeto dbt (dbt_multas): models, macros e testes
├── notebook_qa/                # notebook de execução do dbt + logs
├── src/
│   ├── common/                 # módulo compartilhado entre os pipelines
│   │   ├── config.py           #   constantes: caminhos e nomes de tabelas (fonte única de verdade)
│   │   ├── spark_utils.py      #   SparkSession compartilhada
│   │   ├── logging_utils.py    #   logging padronizado
│   │   └── io_delta.py         #   escrita Delta (overwrite + overwriteSchema) e métricas via SQL
│   ├── bronze_pipeline/        # bronze_lake.py — CSV → Delta
│   ├── silver_pipeline/        # silver_lake.py — regras de negócio e grain
│   └── gold_pipeline/          # gold_lake.py — métricas SQL + parquet
├── tests/                      # suíte pytest (24 testes) — ver tests/README.md
├── .gitignore
└── README.md
```

###  Módulo comum (`src/common`)

Todo o "encanamento" (plumbing) do pipeline foi extraído para um módulo compartilhado:

- **`config.py`** — único lugar para mudar quando o catálogo ou o diretório de dados mudar;
- **`io_delta.py`** — `write_delta_table()` (overwrite com `overwriteSchema` quando a tabela já existe) e `write_metric_from_sql()`;
- **`spark_utils.py`** / **`logging_utils.py`** — sessão Spark e logging padronizados.

Cada pipeline é composto por **funções puras e testáveis** + um `main()` como orquestrador — importar os módulos **não executa** o pipeline (guard `if __name__ == "__main__"`).

---

##  Como Executar

**Pré-requisitos**: workspace Databricks com compute habilitado para Spark e os arquivos CSV em `data/`.

Os pipelines devem ser executados **na ordem** (cada camada consome a anterior):

```text
1. src/bronze_pipeline/bronze_lake.py
2. src/silver_pipeline/silver_lake.py
3. src/gold_pipeline/gold_lake.py
```

Cada script pode ser executado como arquivo no Databricks (o `sys.path` já aponta para `src/`). As tabelas são gravadas no schema **`multas_analytics`** (catálogo `workspace`) — tudo centralizado em `src/common/config.py`.

---

##  Testes

A suíte pytest em `tests/` cobre as três camadas — **24 testes, todos passando**:

- **Bronze**: sanitização de colunas (incluindo cabeçalhos reais da RENAINF), schema estrito e leitura de CSV;
- **Silver**: todas as funções puras — renomeação, filtro de nulos, `QUANTIDADE_AJUSTADA`, dedup de snapshots (mantém o mais antigo), agregação por grain, `ID_MULTA`, datas, dimensão de municípios e construção da gold;
- **Gold**: as regras de negócio do SQL **executadas de verdade** contra DataFrames toy (thresholds 50.000/20.000, janela de datas, filtro `VENCIDA`/`PAGA`) + o export parquet.

Os testes **nunca escrevem** nas tabelas reais: usam temp views descartáveis e o `tmp_path` do pytest. Instruções de execução (local e Databricks) em [`tests/README.md`](tests/README.md).

---

##  dbt — Camada Analítica

O projeto inclui o projeto dbt **`dbt_multas`** como camada analítica complementar, consumindo as tabelas do pipeline:

- **Models**: `quantity_municipios` (silver) · `quantity_per_vehicle_type`, `percentage_per_vehicle`, `quantity_per_month_paid` (gold);
- **Macros**: `sum_columns` (soma dinâmica de colunas);
- **Tests dbt**: unicidade (ex.: `TIPO_VEICULO` em `quantity_per_vehicle_type`) e campos obrigatórios não nulos.

A execução é feita pelo notebook em `notebook_qa/`, com logs versionados em `notebook_qa/logs/`.

---

##  Objetivos do Projeto

- Demonstrar uma implementação prática e completa da arquitetura Medallion;
- Aplicar técnicas modernas de Data Engineering com Databricks, Delta Lake e Unity Catalog;
- Integrar PySpark, SQL, dbt e pytest em um único pipeline;
- Implementar boas práticas de modelagem, testabilidade e qualidade de dados;
- Criar uma solução analítica ponta a ponta baseada em dados públicos reais.

---

##  Autor

**Ricardo Souza Hernandes**

Projeto desenvolvido para fins de estudo, portfólio e demonstração de conhecimentos em:

Data Engineering · Databricks · Delta Lake · PySpark · SQL · dbt · Data Modeling · Data Quality · Analytics Engineering
