from pysus import SIH
import pandas as pd
import os
import glob

#download
sih = SIH().load()
files = sih.get_files("RD", uf="MG", year=2022)
sih.download(files)

#transformar em dfs
dfs = []
for pasta in os.listdir("pysus"):
    caminho_pasta = os.path.join("pysus", pasta)
    if os.path.isdir(caminho_pasta):
        arquivos_parquet = glob.glob(os.path.join(caminho_pasta, "*.parquet"))
        for arquivo in arquivos_parquet:
            df = pd.read_parquet(arquivo)  # Ler o arquivo Parquet
            dfs.append(df)

#juntar os dfs
df_final = pd.concat(dfs, ignore_index=True)
df_final.head()
list(df_final.columns)
df_final.describe
df_final.shape

#filtrando para IAM e AVC:
diag_codigos = ['I21', 'I22', 'I23', 'I61', 'I62', 'I64']
df_filtrado = df_final[df_final['DIAG_PRINC'].str.startswith(tuple(diag_codigos))]

#criando um df com algumas variáveis agregadas para município:
df_municipio = df_filtrado.groupby('res_MUNCOD').agg(
    MUNIC_RES=('MUNIC_RES', 'first'),  # Nome do município
    qtd_internacoes=('res_MUNCOD', 'size'),  # Quantidade de internações
    qtd_sexo_reportado=('SEXO', lambda x: x.isin([0, 1]).sum()),  # Quantidade de internações com sexo reportado
    qtd_sexo_1=('SEXO', lambda x: (x == 1).sum()),  # Quantidade de internações com sexo = 1
    media_diarias=('QT_DIARIAS', 'mean'),  # Média de diárias por internação
    media_us_tot=('US_TOT', 'mean'),  # Média do valor total em dólares
    media_dias_perm=('DIAS_PERM', 'mean'),  # Média do tempo de permanência
    qtd_morte_reportada=('MORTE', lambda x: x.isin([0, 1]).sum()),  # Número de internações com morte preenchida
    qtd_mortes=('MORTE', 'sum'),  # Número de mortes
    media_num_filhos=('NUM_FILHOS', 'mean'),  # Média do número de filhos
    media_idade=('IDADE', 'mean'),  # Média da idade dos internados
    qtd_raca_cor_reportada=('RACA_COR', lambda x: x.notnull().sum()),  # Quantidade de internações com raça/ cor preenchida
    qtd_internacoes_raca_cor=('RACA_COR', 'size'),  # Quantidade de internações por valor de RACA_COR
    qtd_instru_reportada=('INSTRU', lambda x: x.notnull().sum()),  # Quantidade de internações com grau de instrução preenchido
    qtd_internacoes_instru=('INSTRU', 'size')  # Quantidade de internações por valor de INSTRU
).reset_index()
