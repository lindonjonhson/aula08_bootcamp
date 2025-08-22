import pandas as pd
import os
import glob

#   funcao que le e consolida os json

def extrair_dados_e_consolidar(pasta: str) -> pd.DataFrame:

    #   Olhar a pasta 'data' e liste os arquivos json
    arquivos_json = glob.glob(os.path.join(pasta, '*.json'))
    # print(arquivos_json)

    df_list = [pd.read_json(arquivo) for arquivo in arquivos_json]
    # print(df_list)

    #   Concatenando os dataframes
    df_total = pd.concat(df_list, ignore_index=True)
    # print(df_total)

    return df_total

# print(extrair_dados(pasta=pasta))

#   funcao que transforma

def calcular_kpi_de_total_vendas(df: pd.DataFrame) -> pd.DataFrame:
    df['Total'] = df['Quantidade'] * df['Venda']
    return df

#   funcao que load em csv/parquet
#   definir se vai ser csv, parquet ou os 2

def carregar_dados(df: pd.DataFrame, formato_saida: list):
    for formato in formato_saida:
        if formato == 'csv':
            df.to_csv('dados.csv', index=False)
        if formato == 'parquet':
            df.to_csv('dados.parquet')

def pipeline_usuario_final(pasta: str, formato_de_saida: list):

    # pasta_argumento = 'data'

    dataframe = extrair_dados_e_consolidar(pasta)

    dataframe_calculado = calcular_kpi_de_total_vendas(df=dataframe)

    # formato_de_saida: list = ['csv', 'parquet'] 

    carregar_dados(dataframe_calculado, formato_de_saida)



# if __name__ == '__main__':

    # pasta_argumento = 'data'
    # formato_de_saida: list = ['csv', 'parquet'] 
    # pipeline_usuario_final(pasta_argumento, formato_de_saida)