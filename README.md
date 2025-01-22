# Projeto ETL - Extração, Transformação e Carregamento de Dados

Este projeto realiza o processo completo de **ETL** (Extração, Transformação e Carregamento) de dados. O objetivo principal é coletar dados de fontes externas, tratá-los conforme necessário, e carregá-los em um formato adequado para análise ou consumo por outras aplicações.

O processo ETL é dividido em três etapas principais:

1. **Extração**: Coleta os dados de fontes externas, como APIs, bancos de dados ou arquivos, e os armazena localmente em um formato bruto (CSV) na pasta `bronze`.
   
2. **Transformação**: Realiza a limpeza e o tratamento dos dados extraídos. Durante esta etapa, o projeto pode lidar com valores nulos, converter tipos de dados, realizar agregações, e outras manipulações necessárias para transformar os dados brutos em um formato mais útil e estruturado. O resultado da transformação é salvo na pasta `silver`.

3. **Carregamento**: Os dados transformados são carregados para um sistema de armazenamento final, como um banco de dados ou outra aplicação de análise. A versão final dos dados é salva em formato CSV na pasta `gold`, representando dados prontos para análise ou consumo.

## Estrutura do Projeto

├── src                           # Código fonte do projeto
│   ├── etl                       # Módulo ETL
│   │   ├── extract.py            # Script de extração dos dados
│   │   ├── transform.py          # Script de transformação dos dados
│   │   ├── load.py               # Script de carregamento dos dados
│   ├── scheduler                 # Módulo de agendamento
│   │   ├── scheduler.py          # Script para agendar a execução do processo ETL
│   ├── main.py                   # Arquivo principal que executa o processo ETL manualmente
├── data                          # Pasta para armazenar os arquivos de dados (bronze, silver, gold)
│   ├── bronze                    # Dados brutos extraídos (CSV)
│   ├── silver                    # Dados transformados (CSV)
│   └── gold                      # Dados carregados prontos para análise (CSV)
├── venv                          # Ambiente virtual do projeto
├── etl_schedule.log              # Arquivo de log que armazena as execuções do ETL
├── .gitignore                    # Arquivo para ignorar e não ser guardados no git
├── Requirements.txt              # Arquivo de dependências do projeto
└── README.md                     # Este arquivo de documentação (README)

## Como rodar o Projeto
1. Clone o repositório
2. Configure o ambiente virtual:
    # Para sistemas Unix (Linux/Mac)
    python -m venv venv
    source venv/bin/activate

    # Para Windows
    python -m venv venv
    venv\Scripts\activate
3. Instale as dependências
    pip install -r Requirements.txt
4. Escolha por:
    - Rodar o processo ETL manualmente: `python src/main.py`
    - Agendar a execução do processo ETL: `python src/scheduler/scheduler.py`
5. Verifique os logs caso queira




