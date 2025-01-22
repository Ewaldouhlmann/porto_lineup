"""
    Este arquivo executa o processo completo de ETL (Extração, Transformação e Carregamento) dos dados.

    O processo de ETL é dividido em três etapas principais:

    1. **Extração**: A primeira etapa consiste em extrair os dados das fontes dados. Os dados extraídos são então salvos em formato CSV na pasta 'bronze', \
     mantendo os dados brutos para futuras transformações.

    2. **Transformação**: A etapa de transformação realiza a limpeza e a formatação dos dados extraídos. 
       Isso pode incluir a remoção de valores nulos, conversão de tipos de dados, agregações, e outras manipulações necessárias.
       O resultado dessa etapa é salvo em um arquivo CSV na pasta 'silver', representando a versão limpa e transformada dos dados.

    3. **Carregamento**: Após a transformação, os dados são carregados para o sistema de destino, que pode ser um banco de dados ou qualquer outro sistema de armazenamento.
       Os dados finais são salvos em um arquivo CSV na pasta 'gold', representando os dados prontos para análise ou consumo por outras aplicações.

    Este arquivo realiza o processo manual, dentro da pasta scheduler existe um exemplo de como automatizar esse processo utilizando a biblioteca schedule.
"""
from etl.extract import DataExtractor
from etl.transform import DataTransform
from etl.load import DataLoader
import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
    )

def main():
    try:
        data_extractor = DataExtractor()

        logging.info("Iniciando processo de extração.")
        # Executa o processo de extração
        data_extractor.execute_bronze_process()
        logging.info("Extração de dados concluída.")

        current_date = pd.to_datetime("today").strftime("%Y-%m-%d")
        data_transform = DataTransform(current_date)
        logging.info("Iniciando processo de transformação.")
        # Executa o processo de transformação
        data_transform.execute_silver_process()

        loader = DataLoader(current_date)
        logging.info("Iniciando processo de carregamento.")
        # Executa o processo de carregamento
        loader.execute_gold_process()
        logging.info("Carregamento de dados concluído.")
        logging.info("Processo de ETL concluído com sucesso.")
    except Exception as e:
        logging.error(f"Erro durante o processo de ETL: {e}")

if __name__ == "__main__":
    main()

    

