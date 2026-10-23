# es-energy-trade-lakehouse — Contexto do Projeto

## O que é
Projeto de portfólio de Data Engineering do Eduardo Martins Neto, para mostrar a recrutadores e entrevistadores técnicos do mercado espanhol.
Pergunta de negócio: **de onde a Espanha importa petróleo bruto e gás natural, e como essa mistura mudou desde 2022?** (série a partir de 2019).

## O que o projeto precisa provar (ADR-001)
Python/PySpark escrito por ele, lakehouse Databricks (Bronze/Silver/Gold em Delta, Unity Catalog), carga incremental com `MERGE` e tratamento de revisões, qualidade de dados como "contrato" antes de publicar, orquestração como código — e fechar o ciclo num relatório Power BI.
ADF fica de fora de propósito: já está provado por 6 anos de experiência no CV.

## Decisões
Todas as decisões, com motivo e alternativas descartadas, estão em `docs/decisions.md`. Plano e prazos em `docs/plan.md`.
Não mudar uma decisão sem registrar a nova entrada no log.

## Stack
Azure Databricks (PySpark, Delta, Unity Catalog, Lakeflow Jobs, Asset Bundles) · ADLS Gen2 · Python (requests, BeautifulSoup) · Eurostat REST API · pytest + GitHub Actions · Power BI Desktop · Databricks AI/BI (dashboard de saúde do pipeline).

## Estrutura
```
config/sources.yml          # fontes e parâmetros (metadata-driven)
src/es_energy_trade/        # lógica testável (ingestion, transforms, quality)
notebooks/                  # notebooks finos que chamam src/
tests/                      # pytest (roda fora do Databricks, no GitHub Actions)
dashboards/powerbi/         # .pbix + exports
data/samples/               # amostras locais da fase 0 (fora do git)
docs/                       # decisions.md, plan.md
```

## Regras para o Claude
- Documentação do repositório (README, docs, código, commits) em **inglês** — o repo é público e o público são recrutadores. Conversa com o Eduardo em português.
- **Nunca inventar resultados**: README e docs só afirmam números/achados que o pipeline realmente produziu. Enquanto não houver, marcar como pendente.
- Lógica de negócio (HHI, preço implícito, participações) fica no Gold, **nunca em DAX**.
- Transformações como funções puras em `src/` com teste; notebooks só orquestram.
- Orçamento: até **US$ 20/mês** depois dos créditos. Créditos Azure (€172) vencem em **2026-10-29**. Todo cluster com auto-terminação; nada sempre ligado.
- A pasta fica dentro do OneDrive: cuidado com `.git` sincronizado (conflitos/locks). Se der problema, mover o repo para fora do OneDrive.
- Ao citar este projeto no CV/LinkedIn (projeto job-search), descrever só o que estiver implementado.
