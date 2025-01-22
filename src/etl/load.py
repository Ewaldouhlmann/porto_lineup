import pandas as pd
import os

class DataLoader:
    def __init__(self, current_date):
        self.root_path = "../../data/silver/"
        self.arquivo = f"{current_date}.csv"
        self.output_path = "../../data/gold/"

    def load_data(self):
        """
            Carrega os dados da pasta silver em um DataFrame.

            Returns:
                pd.DataFrame: DataFrame com os dados da pasta silver.
        """
        root_path = os.path.join(self.root_path)
        # Verifica se a pasta silver existe
        if not os.path.exists(root_path):
            raise FileNotFoundError(f"Folder {root_path} not found.")
        else:
            files = os.listdir(root_path)
            if self.arquivo in files:
                file_path = os.path.join(root_path, self.arquivo)

                df = pd.read_csv(file_path)
                df_cleaned = df.dropna(subset=['Chegada'])

                
                # Agregando os dados por local, 
                df_agg = df.groupby(['local', 'Sentido', 'Mercadoria'])['Peso'].sum().reset_index()
                
                # COntando o número de veiculos que fizeram importação e exportação para tal local e mercadoria
                df_count = df.groupby(['local', 'Sentido', 'Mercadoria']).size().reset_index(name='Total')

                # Junta os DataFrames
                df_final = pd.merge(df_agg, df_count, on=['local', 'Sentido', 'Mercadoria'], how='outer')

                return df_final
            
    def save_transformed_data(self, df):
        """
            Salva os dados transformados em um arquivo CSV na pasta gold.

            Args:
                df (pd.DataFrame): DataFrame a ser salvo.
        """
        if not os.path.exists(self.output_path):
            os.makedirs(self.output_path)
        
        df.to_csv(os.path.join(self.output_path, self.arquivo), index=False, header=True)
    
    def execute_gold_process(self):
        """
            Realiza todas as transformações nos dados e os salva no diretório final.

            1. Combina os dados da pasta bronze em um único DataFrame e adiciona o local de origem.
        """
        df = self.load_data()
        self.save_transformed_data(df)

