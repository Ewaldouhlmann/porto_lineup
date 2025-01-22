"""
    Este script apresenta a ideia para a execução de um processo de ETL (Extração, Transformação e Carregamento) \
        de dados de forma automatizada, utilizando a biblioteca schedule.

"""

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src')))
import schedule
import logging
import pandas as pd
from etl.extract import DataExtractor
from etl.transform import DataTransform
from etl.load import DataLoader
import time

# Configuração do logging para armazenar logs em arquivo e mostrar no console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("etl_schedule.log"),  # Armazenar logs em um arquivo
        logging.StreamHandler()  # Exibir logs no console
    ]
)

def job():
    """
        Função responsável por executar todo o processo de ETL.
    """
    try:
        current_date = pd.to_datetime("today").strftime("%Y-%m-%d")
        print("Executando ETL...")
        # Executa o processo de extração
        extractor = DataExtractor()
        logging.info("Iniciando processo de extração.")
        extractor.execute_bronze_process()
        logging.info("Processo de extração concluído.")

        # Executa o processo de transformação
        data_transform = DataTransform(current_date)
        logging.info("Iniciando processo de transformação.")
        data_transform.execute_silver_process()
        logging.info("Processo de transformação concluído.")

        # Executa o processo de carga
        loader = DataLoader(current_date)
        logging.info("Iniciando processo de carga.")
        loader.execute_gold_process()
        logging.info("Processo de ETL finalizado com sucesso.")

    except Exception as e:
        logging.error(f"Error: {e}")

def schedule_etl():
    """
        Função responsável por agendar a execução do processo de ETL para ser executado todos os dias.
    
    """
    # Configura o agendamento
    schedule.every().day.at("00:00").do(job)

    # Executa o agendamento, e verifica a cada minuto se há tarefas a serem executadas
    while True:
        print("Executando agendamento...")
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    schedule_etl()
