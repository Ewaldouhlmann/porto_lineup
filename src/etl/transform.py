import pandas as pd
import os

class DataTransform:
    def __init__(self, current_date):
        self.root_path = "../../data/bronze/"
        self.output_path = "../../data/silver/"
        self.current_date = current_date

    def join_data(self):
        """
            Combina os dados da pasta bronze em um único DataFrame e adiciona o local de origem.
        """
        files = os.listdir(self.root_path)
        processed_data = []

        for file in files:
            bronze_files = os.listdir(os.path.join(self.root_path, file))

            if self.current_date in bronze_files:
                daily_files_path = os.path.join(self.root_path, file, self.current_date)
                bronze_files = os.listdir(daily_files_path)

                for bronze_file in bronze_files:
                    file_path = os.path.join(daily_files_path, bronze_file)

                    if os.path.isfile(file_path):
                        df = pd.read_csv(file_path)
                        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
                        df["local"] = file
                        processed_data.append(df)

        return pd.concat(processed_data, ignore_index=True)

    def remove_duplicates(self, df):
        """
            Remove itens duplicados no DataFrame.

            Args:
                df (pd.DataFrame): DataFrame a ser processado.
        """
        return df.drop_duplicates()

    def process_operat_column(self, df):
        """
            Processa a coluna 'Operaç Operat', criando ou ajustando a coluna 'Sentido'.

            Args:
                df (pd.DataFrame): DataFrame com a coluna 'Operaç Operat'.

            Returns:
                pd.DataFrame: DataFrame com a coluna 'Sentido' ajustada.
        """
        if 'Operaç Operat' in df.columns:
            if 'Sentido' not in df.columns:
                df['Sentido'] = None

            df['Sentido'] = df.apply(
                lambda row: (
                    'Imp' if row['Operaç Operat'] == 'EMB' else 
                    'Exp' if row['Operaç Operat'] == 'DESC' else 
                    'Imp/Exp' if pd.isna(row['Sentido']) else row['Sentido']
                ), axis=1
            )
            df = df.drop(columns=['Operaç Operat'])
        return df

    def process_mercadoria_column(self, df):
        """
            Transfere os dados de 'Mercadoria Goods' para a coluna 'Mercadoria'.

            Args:
                df (pd.DataFrame): DataFrame com os dados de 'Mercadoria Goods'.
        """
        if 'Mercadoria Goods' in df.columns:
            df['Mercadoria'] = df.apply(
                lambda row: row['Mercadoria Goods'] if pd.isna(row['Mercadoria']) else row['Mercadoria'], axis=1
            )
            df = df.drop(columns=['Mercadoria Goods'])
        return df

    

    def process_chegada_column(self, df):
        """
            Transfere os dados de 'Cheg/Arrival d/m/y' para a coluna 'Chegada'.

            Args:
                df (pd.DataFrame): DataFrame a ser processado.
        """
        if 'Cheg/Arrival d/m/y' in df.columns:
            df['Chegada'] = df.apply(
                lambda row: row['Cheg/Arrival d/m/y'] if pd.isna(row['Chegada']) else row['Chegada'], axis=1
            )
            df = df.drop(columns=['Cheg/Arrival d/m/y'])
        return df

    def clean_and_covert_weight_column(self, value):
        """
            Limpa e converte o valor da coluna 'Peso Weight' para float.
        
            Args:
                value (str): Valor da coluna 'Peso Weight'.

            Returns:
                float: Valor da coluna 'Peso Weight' convertido para float.
        """
        if isinstance(value, str):
            value = value.replace(' Tons.', '').replace(',', '')
            try:
                if len(value.split()) > 1:
                    return float(sum(map(float, value.split())))/len(value.split())
                return float(value)
            except ValueError:
                return None
        



    def process_saldo_column(self, df):
        """
            Transfere os dados de 'Peso Weight' para a coluna 'Saldo Total'.

            Args:
                df (pd.DataFrame): DataFrame a ser processado.

            Returns:
                pd.DataFrame: DataFrame processado.
        """
        if 'Peso Weight' in df.columns:
            df['Previsto'] = df.apply(
                lambda row: self.clean_and_covert_weight_column(row['Peso Weight']) if pd.isna(row['Previsto']) else self.clean_and_covert_weight_column(row['Previsto']), axis=1
            )
        return df

    def reorder_columns(self, df):
        """
            Reordena as colunas do DataFrame para manter somente as desejadas.

            Args:
                df (pd.DataFrame): DataFrame a ser reordenado.
        """
        df = df[['Chegada', 'Sentido', 'local', 'Mercadoria', 'Previsto']]

        df = df.rename(columns={'Previsto': 'Peso'})
        return df

    def save_transformed_data(self, df):
        """
            Salva o DataFrame transformado na pasta silver, com o nome do arquivo sendo a data atual.

            Args:
                df (pd.DataFrame): DataFrame transformado.
        """
        if not os.path.exists(self.output_path):
            os.makedirs(self.output_path)

        # Remove os Previsto para salvar apenas os com valores dentro
        df = df[df['Peso'].notna()]
        df.to_csv(os.path.join(self.output_path, f"{self.current_date}.csv"), index=False, header=True)

    def remove_duplicates(self, df):
        """
            Remove itens duplicados no DataFrame.

            Args:
                df (pd.DataFrame): DataFrame a ser processado.
        """
        return df.drop_duplicates()
    def execute_silver_process(self):
        """
            Realiza todas as transformações nos dados e os salva no diretório final.

            1. Combina os dados da pasta bronze em um único DataFrame e adiciona o local de origem.
            2. Remove itens duplicados no DataFrame.
            3. Transforma as colunas 'Mercadoria' e 'Chegada'
            4. Reordena as colunas do DataFrame para manter somente as desejadas.
            5. Salva o DataFrame transformado na pasta silver.
        """
        df_final = self.join_data()
        df_final = self.remove_duplicates(df_final)
        df_final = self.process_operat_column(df_final)
        df_final = self.process_mercadoria_column(df_final)
        df_final = self.process_chegada_column(df_final)
        df_final = self.process_saldo_column(df_final)
        df_final = self.reorder_columns(df_final)
        self.save_transformed_data(df_final)
