import pandas as pd 
import polars as pl 
from datetime import datetime
import os 
import gc


try:

    print('Obtendo dados')

    ENDERECO_DADOS = r'./dados/'

    inicio = datetime.now()

    lista_arquivos = []

    lista_dir_arquivos = os.listdir(ENDERECO_DADOS) 

    for arquivo in lista_dir_arquivos:
        if arquivo.endswith('.csv'):
            lista_arquivos.append(arquivo)

    print(lista_arquivos)


    for arquivo in lista_arquivos:
        print(f'Processando arquivo {arquivo}')

        df = pl.read_csv(ENDERECO_DADOS + arquivo, separator=';', encoding='iso-8859-1')

        if 'df_bolsa_familia' in locals():
            df_bolsa_familia = pl.concat([df_bolsa_familia, df])
        else:
            df_bolsa_familia =df

        print(df.head())

        del df

        print(df_bolsa_familia.head())

        print(f'Arquivo {arquivo} processados com sucesso!')

        df_bolsa_familia.write_parquet(ENDERECO_DADOS + 'bolsa_familia.parquet')

        del df_bolsa_familia
        
        gc.collect()


    
 





    # ENDERECO_DADOS = r'./dados/'

    hora_import = datetime.now()
    # df_janeiro = pl.read_csv(ENDERECO_DADOS + '202401_NovoBolsaFamilia.csv', separator=';' ,encoding='iso-8859-1')
    # print(df_janeiro.head())

    hora_impressao = datetime.now()

    print(f'Tempo de execução: {hora_impressao - hora_import}')





except ImportError as e:
    print('Erro ao obter dados: ')