# Projeto Delta Lake Medallion - Multas de Trânsito do Estado de São Paulo

## Visão Geral

Este projeto implementa uma arquitetura **Delta Lake Medallion** utilizando **Databricks**, **PySpark**, **SQL** e **dbt**, com o objetivo de processar, transformar e disponibilizar dados relacionados a multas de trânsito pagas e vencidas de todos os municípios do Estado de São Paulo.

A solução foi desenvolvida seguindo boas práticas de **Data Engineering**, organizando os dados em camadas **Bronze**, **Silver** e **Gold**, garantindo rastreabilidade, qualidade, governança e escalabilidade dos dados.

---

## Stack Tecnológica

- Databricks
- Delta Lake
- PySpark DataFrames
- PySpark SQL
- SQL
- dbt (Data Build Tool)
- Git
- GitHub

---

## Arquitetura Medallion

O projeto segue o padrão **Medallion Architecture**, separando os dados em diferentes níveis de maturidade e qualidade.

### Bronze Layer

Camada responsável pela ingestão dos dados brutos.

#### Objetivos

- Receber arquivos CSV originais.
- Preservar os dados sem alterações significativas.
- Garantir rastreabilidade das fontes.

#### Principais Atividades

- Leitura dos arquivos CSV.
- Padronização mínima de colunas.
- Armazenamento em tabelas Delta Bronze.

---

### Silver Layer

Camada responsável pelo tratamento e enriquecimento dos dados.

#### Objetivos

- Corrigir inconsistências.
- Tratar valores nulos.
- Remover duplicidades.
- Aplicar regras de negócio.

#### Principais Atividades

- Limpeza dos dados.
- Padronização de tipos.
- Validação de registros.
- Enriquecimento das informações de multas.

---

### Gold Layer

Camada destinada ao consumo analítico.

#### Objetivos

- Disponibilizar métricas prontas para análise.
- Facilitar integração com ferramentas de BI.
- Suportar consultas de alta performance.

#### Exemplos de Indicadores

- Total de multas emitidas por município.
- Total de multas pagas.
- Total de multas vencidas.
- Percentual de inadimplência.
- Ranking de municípios com maior volume de infrações.
- Evolução temporal das multas.

---

## Estrutura do Projeto

```text
project/
│
├── data/
├── dbt/
├── src/
├── .gitignore
└── README.md
```

---

## Diretório `data`

Responsável pelo armazenamento dos arquivos CSV extraídos e das tabelas geradas ao longo do pipeline de dados.

### Finalidade

- Armazenar arquivos brutos recebidos das fontes.
- Armazenar tabelas Delta das camadas Bronze, Silver e Gold.
- Disponibilizar os dados utilizados durante os processos de ingestão, transformação e análise.

---

## Diretório `dbt`

Responsável pela camada de transformação analítica, modelagem e governança dos dados.

### Estrutura

```text
dbt/
├── models/
├── macros/
└── tests/
```

### Models

Contém os modelos SQL responsáveis por transformar e organizar os dados para consumo analítico.

### Macros

Funções reutilizáveis utilizadas para padronização de código e implementação de regras de negócio.

### Tests

Validações automatizadas para garantir qualidade, consistência e integridade dos dados.

#### Validações Implementadas

- Campos obrigatórios não nulos.
- Unicidade de identificadores.
- Integridade referencial.
- Consistência de dados entre camadas.
- Regras de negócio específicas do domínio de multas de trânsito.

---

## Diretório `src`

Contém toda a lógica de engenharia de dados implementada em **PySpark DataFrames** e **PySpark SQL**.

### Estrutura

```text
src/
├── bronze/
├── silver/
└── gold/
```

---

### Bronze

Scripts responsáveis pela ingestão dos dados brutos.

#### Processos

- Leitura de arquivos CSV.
- Criação de DataFrames.
- Aplicação de padrões iniciais.
- Escrita em tabelas Delta Bronze.

---

### Silver

Scripts responsáveis pela transformação e tratamento dos dados.

#### Processos

- Tratamento de dados faltantes.
- Padronização de colunas.
- Conversão de tipos.
- Aplicação de regras de negócio.
- Remoção de registros inválidos.
- Deduplicação de registros.

---

### Gold

Scripts responsáveis pela construção das tabelas analíticas.

#### Processos

- Agregações.
- Criação de métricas.
- Construção de tabelas analíticas.
- Geração de datasets para dashboards e relatórios.

---

## Fluxo do Pipeline

```text
CSV Files
    │
    ▼
Bronze Layer
(Ingestão)
    │
    ▼
Silver Layer
(Limpeza e Transformação)
    │
    ▼
Gold Layer
(Métricas e Agregações)
    │
    ▼
dbt Models
    │
    ▼
Analytics / Dashboards
```

---

## Objetivos do Projeto

- Demonstrar uma implementação prática da arquitetura Medallion.
- Aplicar técnicas modernas de Data Engineering.
- Utilizar Delta Lake para processamento confiável e escalável.
- Integrar Databricks, PySpark, SQL e dbt em um único pipeline.
- Implementar boas práticas de modelagem e qualidade de dados.
- Criar uma solução analítica ponta a ponta baseada em dados públicos de multas de trânsito do Estado de São Paulo.

---

## Autor

**Ricardo Souza Hernandes**

Projeto desenvolvido para fins de estudo, portfólio e demonstração de conhecimentos em:

- Data Engineering
- Databricks
- Delta Lake
- PySpark
- SQL
- dbt
- Data Modeling
- Data Quality
- Analytics Engineering
