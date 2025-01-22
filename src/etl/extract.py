import pandas as pd
import requests
from io import StringIO
import os

class DataExtractor:
    def __init__(self):
        """
        Inicializa o objeto DataExtractor com as URLs das páginas
        de onde os dados serão extraídos. Essas URLs estão associadas
        às cidades de Santos e Paranaguá.
        """
        self.urls = {
            "santos": "https://www.portodesantos.com.br/informacoes-operacionais/operacoes-portuarias/navegacao-e-movimento-de-navios/navios-esperados-carga/",
            "paranagua": "https://www.appaweb.appa.pr.gov.br/appaweb/pesquisa.aspx?WCI=relLineUpRetroativo"
        }

    def extract(self, city):
        """
        Função responsável por decidir qual método de extração utilizar
        com base na cidade informada e retornar os dados extraídos.

        Args:
            city (str): Nome da cidade de onde os dados devem ser extraídos (santos ou paranagua).

        Returns:
            pd.DataFrame: DataFrame contendo os dados extraídos da página correspondente
            à cidade escolhida, ou gera um erro caso a cidade não seja válida.
        
        Raises:
            ValueError: Se a cidade informada não estiver entre as opções disponíveis.
        """
        try:
            # Verifica se a cidade informada é válida
            if city not in self.urls:
                raise ValueError(f"Cidade {city} não encontrada nas fontes de dados.")
            
            # Chama a função apropriada para extrair dados com base na cidade
            if city == "santos":
                return self._extract_santos()
            elif city == "paranagua":
                return self._extract_paranagua()
        except Exception as e:
            # Em caso de erro, a exceção é capturada e re-levantada
            raise e
        
    def _extract_santos(self):
        """
        Função privada responsável por extrair dados da página de Santos.

        Esta função faz uma requisição HTTP para a página do Porto de Santos,
        obtém o conteúdo HTML e converte as tabelas presentes na página
        para DataFrames do pandas.

        Returns:
            list: Lista de DataFrames contendo os dados extraídos da página de Santos.

        Raises:
            Exception: Caso ocorra algum erro na requisição ou processamento dos dados.
        """
        try:
            # Requisição HTTP para a página de Santos
            response = requests.get(self.urls["santos"], verify=False)

            # Io para tratar o conteúdo HTML como arquivo
            str_response = StringIO(response.text)

            # Converte o conteúdo HTML em tabelas de DataFrames
            data = pd.read_html(str_response)
            return data
        except Exception as e:
            # Captura qualquer erro que ocorra durante a extração
            raise e
    
    def _extract_paranagua(self):
        """
        Função privada responsável por extrair dados da página de Paranaguá.

        Esta função faz uma requisição HTTP para a página de Paranaguá,
        obtém o conteúdo HTML e converte as tabelas presentes na página
        para DataFrames do pandas.

        Returns:
            list: Lista de DataFrames contendo os dados extraídos da página de Paranaguá.

        Raises:
            Exception: Caso ocorra algum erro na requisição ou processamento dos dados.
        """
        try:
            # Requisição HTTP para a página de Paranaguá
            response = requests.get(self.urls["paranagua"])
            str_response = StringIO(response.text)

            # Converte o conteúdo HTML em tabelas de DataFrames
            data = pd.read_html(str_response, header=None, index_col=0)

            # Valida antes de definir o cabeçalho
            validated_data = self._validate_paranagua_data_frame(data)

            return validated_data
        except Exception as e:
            # Captura qualquer erro que ocorra durante a extração
            raise e

    def _validate_paranagua_data_frame(self, data_frame_list: list):
        """
        Essa função garante que apenas os dataframes não nulos e do modelo de dados esperado sejam retornados.

        Args:
            data_frame_list (list): Lista de DataFrames extraídos da página de Paranaguá.
        """
        valid_data_frames = []

        for data_frame in data_frame_list:
            if data_frame.empty or len(data_frame) < 2:
                continue
            # Verifica a estrutura ou condição específica
            if data_frame.columns[0][1].upper() == "PROGRAMAÇÃO":
                valid_data_frames.append(data_frame)

        return valid_data_frames



    def execute_bronze_process(self):
        """
            Função responsável por executar o processo de extração de dados das cidades de Santos e Paranaguá, \
            salvando os resultados em arquivos CSV, contendo a data e o tipo da planilha no diretório de saída (data/bronze).
        """
        try:
            # Pega o dia atual
            today = pd.Timestamp.today().date()

            # Cria a pasta raiz data se ela não existir
            if not os.path.exists("../../data"):
                os.makedirs("../../data")

            for city in self.urls:
                data = self.extract(city)
                
                # Cria a pasta data/bronze se ela não existir
                if not os.path.exists("../../data/bronze"):
                    os.makedirs("../../data/bronze")
                
                # Cria as pastas data/bronze/santos e data/bronze/paranagua
                if not os.path.exists(f"../../data/bronze/{city}"):
                    os.makedirs(f"../../data/bronze/{city}")

                # Cria a pasta para o dia atual se ela não existir
                today_directory = f"../../data/bronze/{city}/{today}"
                if not os.path.exists(today_directory):
                    os.makedirs(today_directory)
                    
                # Salva os dados em arquivos CSV
                for i, df in enumerate(data):
                    # Nome do arquivo: substitui espaços por underscores e remove acentos
                    filename = df.columns[0][0].replace(" ", "_").lower()
                    
                    # Configura para pegar apenas o segundo nível do MultiIndex (O primeiro é o titulo da coluna)
                    df.columns = df.columns.get_level_values(1)
                    df.reset_index(drop=True, inplace=True)

                    # Caminho completo para o arquivo CSV
                    file_path = f"{today_directory}/{filename}.csv"

                    # Cria a pasta do arquivo CSV se ela não existir
                    if not os.path.exists(os.path.dirname(file_path)):
                        os.makedirs(os.path.dirname(file_path))

                    # Salva o DataFrame no arquivo CSV
                    df.to_csv(file_path, index=False)

        except Exception as e:
            # Em caso de erro, a exceção é capturada e re-levantada
            raise e
        