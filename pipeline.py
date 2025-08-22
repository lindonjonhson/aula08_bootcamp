from etl import pipeline_usuario_final


pasta_argumento = 'data'
formato_de_saida: list = ['csv', 'parquet'] 

pipeline_usuario_final(pasta_argumento, formato_de_saida)