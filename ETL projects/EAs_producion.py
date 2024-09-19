#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sys
sys.path.append(r"D:\Scripts1\Code\ActPEA\CODE\Actu_Colums.py")
import pandas as pd
import matplotlib as mptl
import Actu_Colums as Ac
from openpyxl import load_workbook
from typing import Optional
import glob,os,re
import numpy as np
from collections import defaultdict
from babel.dates import format_date, format_datetime, Locale
import datetime
from sklearn.preprocessing import OneHotEncoder
import locale
locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')
import pickle
import subprocess


# # Funciones 

# In[2]:


## Variablez iniciales
Today_date = datetime.date.today()
Today_str = datetime.date.today().strftime('%d-%m-%Y')
filas_normal = Today_date
Today_D_M = Today_str[0:5]
Today_D_M = [Today_D_M[0:2],'.',Today_D_M[3:6]]
Today_D_M = ''.join(Today_D_M)
with open(r"\\LIMBIPBICOV01.claro.pe\Red Región Norte\PowerBI\Dimensiones\temp.pkl", "rb") as f:
    filas_count = pickle.load(f)
def cortar_hora(fecha_hora_str):
    return fecha_hora_str.split(" ")[0]
def addSite(EA_act_df : pd.DataFrame, SAP_4_use : pd.DataFrame):
    """Funcione que toma las tablas SAP y extrae los sites y los codigos de AHI
        Devuelve un DF con la Informacion Añadida ahi"""
    df_merged = pd.merge(EA_act_df,SAP_4_use,on='CONCATENADO',how='left')
    df_merged = df_merged.drop_duplicates(subset='CONCATENADO')
    df_merged['PEP Desc'] = df_merged['PEP Desc'].astype(str)
    df_merged['ID_SITE_SAP'], df_merged['SITE_SAP'] = zip(*df_merged['PEP Desc'].apply(lambda x: re.split('(?<=\d)(?=[a-zA-Z])', x) 
                                                                                           if re.search('(?<=\d)(?=[a-zA-Z])', x) else [np.nan,x]))
    # Verifica si las columnas 'SITE' e 'ID_SITIO' existen, si no, las crea
    if 'SITE' not in df_merged.columns:
        df_merged['SITE'] = np.nan
    if 'ID_SITIO' not in df_merged.columns:
        df_merged['ID_SITIO'] = np.nan

    # Reemplaza los valores NaN en 'SITE' e 'ID_SITIO' con los valores de 'SITE_SAP' e 'ID_SITE_SAP'
    df_merged['SITE'] = df_merged['SITE'].combine_first(df_merged['SITE_SAP'])
    df_merged['ID_SITIO'] = df_merged['ID_SITIO'].combine_first(df_merged['ID_SITE_SAP'])

    df_merged.SITE = df_merged.SITE.str.strip() # Quito espacios

    EA_act_df = df_merged.drop(columns=['ID_SITE_SAP','SITE_SAP','Fecha OC']) # Elimino las columnas que use para el Merge 
    return EA_act_df
    
def get_recent_df_B(Carpeta_path: str, sheet_name: str):
    """ Devuelve el df de la hoja especifica, del archivo mas reciente sin guion bajo creado de la carpeta especificada"""
    Path_n= Carpeta_path + '/*'
    tipo_de_archivo = '*.xlsx'
    # Busca el archivo más reciente
    archivos = glob.glob(Path_n + tipo_de_archivo)
    # Filtra los archivos que no contienen "_" en su nombre
    archivos_sin_guion_bajo = [archivo for archivo in archivos if "_" not in os.path.basename(archivo)]
    archivo_mas_reciente = max(archivos_sin_guion_bajo, key=os.path.getctime)
    nombre_del_archivo_N = os.path.basename(archivo_mas_reciente)
    print(archivo_mas_reciente)
    # Lee el archivo sin especificar el tipo de datos
    df = pd.read_excel(archivo_mas_reciente , sheet_name=sheet_name)
    # Si la columna "COMENTARIO" existe, cambia su tipo de datos a str
    if 'COMENTARIO' in df.columns:
        df['COMENTARIO'] = df['COMENTARIO'].astype(str)
    return df,nombre_del_archivo_N
def get_recent_df(Carpeta_path: str, sheet_name: str):
    """ Devuelve el df de la hoja especifica, del archivo mas reciente modificado de la carpeta especificada"""
    Path_n= Carpeta_path + '/*'
    tipo_de_archivo = '*.xlsx'
    # Busca el archivo más reciente
    archivos = glob.glob(Path_n + tipo_de_archivo)
    archivo_mas_reciente = max(archivos, key=os.path.getmtime)
    nombre_del_archivo_N = os.path.basename(archivo_mas_reciente)
    print(archivo_mas_reciente)
    # Lee el archivo sin especificar el tipo de datos
    df = pd.read_excel(archivo_mas_reciente , sheet_name=sheet_name)
    # Si la columna "COMENTARIO" existe, cambia su tipo de datos a str
    if 'COMENTARIO' in df.columns:
        df['COMENTARIO'] = df['COMENTARIO'].astype(str)
    return df,nombre_del_archivo_N



def get_recent_df_by_N(Carpeta_path: str, sheet_name: str, prefijo_nombre_archivo: str):
    """ Devuelve el df de la hoja especifica, del archivo mas reciente de la carpeta especificada, con el nombre especificado"""
    # Busca el archivo más reciente que comienza con el prefijo_nombre_archivo
    archivos = glob.glob(os.path.join(Carpeta_path, prefijo_nombre_archivo + '*.xlsx'))
    archivo_mas_reciente = max(archivos, key=os.path.getctime)
    nombre_del_archivo_N = os.path.basename(archivo_mas_reciente)
    print(archivo_mas_reciente)
    # Lee el archivo sin especificar el tipo de datos
    df = pd.read_excel(archivo_mas_reciente , sheet_name=sheet_name)
    # Si la columna "COMENTARIO" existe, cambia su tipo de datos a str
    if 'COMENTARIO' in df.columns:
        df['COMENTARIO'] = df['COMENTARIO'].astype(str)
    return df,archivo_mas_reciente


def get_recent_csv(Carpeta_path: str):
    """ Devuelve el df del archivo csv más reciente de la carpeta especificada"""
    Path_n = Carpeta_path + '/*'
    tipo_de_archivo = '*.csv'
    # Busca el archivo más reciente
    archivos = glob.glob(Path_n + tipo_de_archivo)
    archivo_mas_reciente = max(archivos, key=os.path.getmtime)
    nombre_del_archivo_N = os.path.basename(archivo_mas_reciente)
    print(archivo_mas_reciente)
    # Lee el archivo sin especificar el tipo de datos
    df = pd.read_csv(archivo_mas_reciente)
    # Si la columna "COMENTARIO" existe, cambia su tipo de datos a str
    if 'COMENTARIO' in df.columns:
        df['COMENTARIO'] = df['COMENTARIO'].astype(str)
    return df, nombre_del_archivo_N

def Process_PAP(PAP:pd.DataFrame):
    PAP_f = PAP[['OC Posición','N° Sol','Estado','Acción','Nombre responsable','Id.SIte',
                 'SIte','# Días','F.Creación','F.Modifica']]
    ##Filtro solo del norte
    PAP_f = PAP_f.dropna(subset=['Id.SIte'])
    PAP_f = PAP_f[PAP_f['# Días'] < 500]
    
    PAP_f_N = PAP_f[PAP_f['Id.SIte'].str.startswith(('L','T','SAD','CL','CAC'))].copy()
    PAP_f_N = PAP_f_N.rename(columns={'N° Sol': 'PAP', #Rename
                                   'SIte': 'SITE',
                                   'Estado': 'ESTADO_PAP',
                                    '# Días': 'ANTIGUAMIENTO_PAP',
                                    'Id.SIte': 'ID Site',
                                 'Nombre responsable': 'RESPONSABLE_PAP' })
    PAP_S = Ac.split_ocs(PAP_f_N) # Spliteo OCs
    return PAP_S
def pre_proces(df: pd.DataFrame ,columns_2str: list[str] ,column_filter: str,C_format) -> pd.DataFrame: 
    """Funcion que filtra y convierte a str ciertas columnas especificadas """
    ## Preprosecing of PREP_NEW
    df = convert_columns(df.copy(),columns_2str,C_format ) #Convert to str a key column
    df_EI = df.loc[df[column_filter] == 'Eduardo Iberico']#Filter
    df_EI = df_EI.copy()  # Crea una copia del DataFrame original para evitar modificar los datos originales
    return df_EI
def convert_columns(df, columns,type):
    for column in columns:
        df[column] = df[column].astype(type)
    return df

def tratar_codigo(codigo):
    # Si el código es vacío o nulo, devolver tal cual
    if pd.isna(codigo) or codigo == '':
        return codigo
    
    # Si el código tiene una letra extra en el medio
    if not re.match(r'^[A-Za-z]{2}\d{4}$', codigo):
        # Quitar la tercera letra si es una letra
        if codigo[2].isalpha():
            codigo = codigo[:2] + codigo[3:]
        # Quitar los números extras al final si son números
        while len(codigo) > 6 and codigo[-1].isdigit():
            codigo = codigo[:-1]
    
    return codigo

def compact_rows(df:pd.DataFrame, columns:list, delimiter:str ='/'):
    """ Entra el DF, Las conlumnas key, y el delimitador"""
    for col in df.columns:
        df[col] = df[col].astype(str)
    # Usar 'join' como función de agregación para concatenar los valores
    agg_func = {col: lambda x: '/'.join(x.unique()) for col in df.columns if col not in  columns}
    
    # Agrupar por 'OC Posición' y aplicar la función de agregación
    df1 = df.groupby(columns).agg(agg_func).reset_index()
    return df1
def contar_prefijos(lista):
        contador = defaultdict(int)
        for cadena in lista:
            for i in range(1, len(cadena) + 1):
                prefijo = cadena[:i]
                contador[prefijo] += 1
        return contador

def normalize_company_names(df, column):
    """Normaliza los nombres de las empresas en la columna especificada del DataFrame."""
    # Reemplaza "SAC" o "S. A. C." al final de los nombres de las empresas con "S.A.C"
    df.loc[:,column] = df[column].str.replace(r"(SAC|S\. ?A\. ?C\.)$", "S.A.C.", regex=True)
    return df


def update_and_rename(df1, update_cols, new_names):
    df = df1.copy()
    for col in update_cols:
        df[col[0]].update(df[col[1]])
    df.rename(columns=new_names, inplace=True)
    df.drop(columns=[col for sublist in update_cols for col in sublist[1:]], inplace=True)
    return df
def combine_and_rename(df1, combine_cols, new_names):
    df = df1.copy()
    for cols in combine_cols:
        df[cols[0]] = df[cols[0]].combine_first(df[cols[1]])
    df.rename(columns=new_names, inplace=True)
    df.drop(columns=[col for sublist in combine_cols for col in sublist[1:]], inplace=True)
    return df
def load_merge(directorio:str):
    # Dicc para almacenar los DataFrames
    dfs = {}
    # Itera sobre todos los archivos en el directorio
    for filename in os.listdir(directorio):
        file_path = os.path.join(directorio, filename)
        # Verifica si el path es un archivo y no un directorio
        if os.path.isfile(file_path):
            df = pd.read_excel(file_path)  
            # Obtiene el nombre del archivo sin la extensión
            nombre_sin_extension = os.path.splitext(filename)[0]
            # Añade el DataFrame al diccionario
            dfs[nombre_sin_extension] = df
    ## Etiqueto cada Df por su temporalidad, añado una columna mas con dicha etiqueta         
    for tiempo, df in dfs.items():
        df['TIME'] = tiempo
    # Combina todos los dataframes en uno solo
    df_combinado = pd.concat(dfs.values())
    return df_combinado 
def clean_nan(df:pd.DataFrame,column:str):
    if filas_count > filas_normal:
        df.loc[:,column] = df.loc[:,column].replace('nan',np.nan)
        return df
    else: return df
def process_to_bcsv(df : pd.DataFrame,ruta_del_csv : str,Fecha: str):
    df['TIME'] = Fecha
    pivot_table = pd.pivot_table(df, values='EN_PROC_USD', index=['TIME','FECHA_DOC','RESPONSABLE_DE_EA','Estado de EA'], aggfunc=pd.Series.sum) #Agrupo 
    df_reset = pivot_table.reset_index(drop=False)
    #convetir la columna del agrupado al formato d efehca 
    # Convierte la columna 'Fecha' a datetime
    df_reset['TIME'] = pd.to_datetime(df_reset['TIME'],format = '%d-%m-%Y')
    
    # Formatea la columna 'Fecha'
    df_reset['TIME Format'] = df_reset['TIME'].apply(lambda x: format_date(x, 'EEE dd-MM-yyyy', locale=Locale('es', 'ES')))
    #return df_reset
    df_reset.to_csv(ruta_del_csv, mode='a', header=False,index=False)
def get_OCS (df:pd.DataFrame): ## Correguir esta funcion(Solo debe hacer una cosa)
    EA_PAP_clean_4_SAP = df[(df.SITE.isna())|(df.SITE == 'nan')] #filto2
    listocs = EA_PAP_clean_4_SAP['CONCATENADO'].astype(str).tolist() # Creo lsitas de OCS
    resultado = [valor[4:] for valor in listocs] # Tomo los valores luego del 4500
    
    a,b= MostCL_prefix(resultado)
    return a,b
def MostCL_prefix(lista : list):
    b = 0
    list_n = []
    list_n1 = {}
    contador = contar_prefijos(lista)
    for key in contador.keys(): 
       ###Aqui iria la nueva condicional### 
        if contador[key] < b:
               list_n.append(key)
        b = contador[key]
    claves = list(contador.keys())
    for clave_dada in list_n:
           indice = claves.index(clave_dada)
           clave_anterior = claves[indice - 1]
           #list_n.append(clave_anterior)
           list_n1[clave_anterior] = contador[clave_anterior]
    return list_n1,contador
def limpiar_id(df, col_id, col_nombre):
    # Convertir la columna de ID a string para poder hacer la comparación
    df[col_id] = df[col_id].astype(str)
    
    # Crear una función para limpiar el nombre
    def limpiar_nombre(row):
        nombre = str(row[col_nombre])  # Convertir el nombre a string
        id_actual = row[col_id]
        
        # Dividir el nombre por "_" ,"-" y " " 
        partes = re.split('_|-| ', nombre)
        
        # Si el ID está en el nombre, eliminarlo
        if id_actual in partes:
            partes.remove(id_actual)
        
        # Devolver el nombre limpio
        return ' '.join(partes)
    
    # Aplicar la función de limpieza a cada fila del DataFrame
    df[col_nombre] = df.apply(limpiar_nombre, axis=1)
    return df
def data_date(path:str):
    modification_time = os.path.getctime(path)
    dt = datetime.fromtimestamp(modification_time)  
    dt = dt.date()
    # Formatea la fecha en el formato deseado
    return dt
def process_2_model(df_real:pd.DataFrame,selector):
    df_real = df_real[['TEXTO','PROVEEDOR','NOMBRE PROYECTO','TIPO_PROYECTOS'
                             ,'CLASIF_RED_1']]
        # Supongamos que 'encoder' es tu OneHotEncoder ya ajustado y 'df_real' es tu DataFrame real
    encoder = OneHotEncoder(sparse_output=False, drop='first')
    encoded_data = encoder.fit_transform(df_real)
    # Aplica el codificador a tus datos reales
    df_real_encoded = encoder.transform(df_real)
    
    # Convierte el resultado en un DataFrame
    df_real_encoded = pd.DataFrame(df_real_encoded, columns=encoder.get_feature_names_out(df_real.columns))


    #@TODO: AÑADIR SELECTOR
    #selector.feature_names_in_.tolist()
    # Asegúrate de que todas las columnas en tus datos de entrenamiento también existen en tus datos reales
    for col in selector.feature_names_in_.tolist():
        if col not in df_real_encoded.columns:
            # Si falta alguna columna en tus datos reales, añade una nueva columna llena de ceros
            df_real_encoded[col] = 0
    # Ordena las columnas de df_real_encoded para que coincidan con el orden de las columnas en df_encoded
    df_real_encoded = df_real_encoded.reindex(columns=selector.feature_names_in_.tolist())
    df_real_encoded_select = selector.transform(df_real_encoded)

    return df_real_encoded_select

def get_PAP_data(path_admin:str,path_otros:str):
    PAP_O,_ = get_recent_df_B(path_otros,'Hoja2')# PAP de Otros
    PAP_A,_ = get_recent_df(path_admin,'Hoja2')# PAP de Admin
    PAP = pd.concat([PAP_O,PAP_A])
    PAP_DB = PAP[['Id.SIte','SIte']].drop_duplicates(subset='Id.SIte').rename(columns={'Id.SIte':'ID_SITIO','SIte':'SITE'})
    return PAP,PAP_DB

def getime_of_report(Path_data:str):
    data_time = data_date(Path_data)
    a = Today_date-data_time
    if data_time == Today_date:
        reporte_old = False
        print("El reporte es actual")
    elif data_time < Today_date:
        reporte_old = True
        print(f"El reporte es de hace {a.days} dias")
    return reporte_old

def check_run(Path_data:str):
    with open(r'\\LIMBIPBICOV01.claro.pe\Red Región Norte\EAS\EAs_done.pkl', "rb") as f:
        lista_EA_done = pickle.load(f)
    if Path_data in lista_EA_done: # compruebo si ya ejecute el script sobre esa base de archivo 
        #global reporte_old
        reporte_old = True
    else: 
        #global reporte_old
        reporte_old = False 
        lista_EA_done.append(Path_data) ##añado a la lista de ya ejecutados 
        with open(r'\\LIMBIPBICOV01.claro.pe\Red Región Norte\EAS\EAs_done.pkl', "wb") as archivo:
            pickle.dump(lista_EA_done,archivo)
    return reporte_old

def load_OTs(path:str):
    # Archivo PAP OTs
    OT, _ = get_recent_df(path, 'Hoja2')
    OT = OT.query("`Status OT` not in ['PDTE CONTRATA', 'PDTE RESPONSABLE']")
    
    # Aplica la función a toda la columna 'Fecha de Creación' y convierte a datetime
    OT.loc[:,'Fecha de Creación'] = pd.to_datetime(OT['Fecha de Creación'].apply(cortar_hora), format='%d/%m/%Y', dayfirst=True)
    
    # Filtra las OTs solo de este año
    fecha_limite = pd.Timestamp('2022-01-01')
    OT_cut = OT[OT['Fecha de Creación'] > fecha_limite].copy()
    
    # Normaliza los nombres de las compañías
    OT_cut = normalize_company_names(OT_cut, 'Contrata')   
    return OT_cut,OT



def PrepOts(OT_cut:pd.DataFrame,OT:pd.DataFrame):
    ## RollOUT
    # Definir proyectos y filtrar DataFrame
    Proyectos = ['ROLLOUT - 2023', 'ROLLOUT - 2022', 'ROLLOUT - 2024']
    OT_cut_RRL = (OT_cut[OT_cut.Etiqueta.isin(Proyectos)]
                  .rename(columns={'Codigo de Site': 'ID_SITIO',
                                   'Contrata': 'PROVEEDOR',
                                   'Nombre de Site': 'SITE'})
                  [['OT', 'ID_SITIO', 'SITE', 'Proyecto', 'PROVEEDOR', 'Status OT']])
    
    # Compactar filas
    OT_agg_ID_PRO_RLL = compact_rows(OT_cut_RRL, ['ID_SITIO', 'PROVEEDOR'], '/')
    OT_agg_ID_NM_PRO_RLL = compact_rows(OT_cut_RRL, ['ID_SITIO', 'SITE', 'PROVEEDOR'], '/')
    
    #MOdernizacion
    # Filtrar DataFrame y renombrar columnas
    OT_cut_M = (OT_cut[OT_cut.Proyecto == 'EXPANSIÓN']
                .rename(columns={'Codigo de Site': 'ID_SITIO',
                                 'Contrata': 'PROVEEDOR',
                                 'Nombre de Site': 'SITE',
                                 'Proyecto': 'RESPONSABLE_DE_EA'})
                .assign(RESPONSABLE_DE_EA='ANGGIE')
                [['OT', 'ID_SITIO', 'SITE', 'RESPONSABLE_DE_EA', 'PROVEEDOR', 'Status OT']])
    
    # Compactar filas
    OT_agg_ID_PRO_M = compact_rows(OT_cut_M, ['ID_SITIO', 'PROVEEDOR'])
    OT_agg_ID_N_PRO_M = compact_rows(OT_cut_M, ['ID_SITIO', 'SITE', 'PROVEEDOR'])
    
    
    ## Energia
    # Definir fecha límite y filtrar DataFrame
    fecha_limite = pd.Timestamp('2023-02-15')
    OT_cut = (OT[OT['Fecha de Creación'] > fecha_limite]
              .pipe(normalize_company_names, 'Contrata'))
    
    # Filtrar por tipo de requerimiento y proyectos de energía
    Proy_energia = ['AC ESTABILIZADA', 'AA', 'INCREMENTO DE POTENCIA (INTERNO - CONSTRUCCIÓN)',
                    'AC COMERCIAL', 'ENERGÍA DC', 'AMPLIACION DE POTENCIA']
    OT_cut_E = (OT_cut[OT_cut['Tipo Req'] == 'MANT. MEJORA DE RED']
                .rename(columns={'Codigo de Site': 'ID_SITIO',
                                 'Contrata': 'PROVEEDOR',
                                 'Nombre de Site': 'SITE',
                                 'Proyecto': 'RESPONSABLE_DE_EA'})
                .query("RESPONSABLE_DE_EA in @Proy_energia")# El @ se usa para referirse a una variable externa
                .assign(RESPONSABLE_DE_EA='JORGE')  
                [['OT', 'ID_SITIO', 'SITE', 'RESPONSABLE_DE_EA', 'PROVEEDOR', 'Status OT']])
    
    # Compactar filas y contar valores de 'Status OT'
    OT_cut_E_ID_PRO = compact_rows(OT_cut_E, ['ID_SITIO', 'PROVEEDOR'])
    
    return OT_agg_ID_PRO_RLL,OT_agg_ID_NM_PRO_RLL,OT_agg_ID_PRO_M,OT_agg_ID_N_PRO_M,OT_cut_E_ID_PRO
    

def PrepPEA(PEA:pd.DataFrame):
    # Preprocesar y normalizar nombres de compañías
    PEA_EI = (pre_proces(PEA, ['DOC_PREC', 'POS_PREC', 'CONCATENADO'], 'SOLICITANTE', 'int64')
              .pipe(normalize_company_names, 'PROVEEDOR')
              .pipe(pre_proces, ['DOC_PREC', 'POS_PREC', 'CONCATENADO'], 'SOLICITANTE', 'str'))
    
    # Crear columna "OC Posición" y calcular monto en USD
    PEA_EI["OC Posición"] = PEA_EI["DOC_PREC"].str.cat(PEA_EI["POS_PREC"], sep=":")
    Cash_In = PEA_EI["EN_PROC_USD"].sum()
    # Tratar los códigos
    PEA_EI['ID_SITIO'] = PEA_EI['ID_SITIO'].apply(tratar_codigo)    
    return PEA_EI,Cash_In

def PrepPext(path:str):
    try:
        PEXT,_ = get_recent_df(path,sheet_name='General') # Ruta archivo pEXT
        PEXT.loc[:,'REQ'] = PEXT.loc[:,'REQ'].fillna(0000)
        # Filtrar y seleccionar columnas
        PEXT_N = (PEXT[PEXT.REGION == 'NORTE']
                  .iloc[:, 1:]
                  [['REQ', 'ID', 'SITE', 'RESPONSABLE DE IMPLEMENTACION', 'CONTRATISTA', 'ESTATUS GENERAL', 'ORDEN DE COMPRA', 'POS']]
                  .astype({'REQ': 'int32', 'ORDEN DE COMPRA': 'str', 'POS': 'str'}))
        
        # Crear columna "OC Posición" y reemplazar valores 'nan'
        PEXT_N["OC Posición"] = PEXT_N["ORDEN DE COMPRA"].str.cat(PEXT_N["POS"], sep=":")
        PEXT_N.replace({'ORDEN DE COMPRA': {'nan': np.nan}, 'POS': {'nan': np.nan}, 'OC Posición': {'nan:nan': np.nan}}, inplace=True)
        
        # Eliminar columnas y filtrar filas no nulas
        PEXT_N.drop(columns=['ORDEN DE COMPRA', 'POS'], inplace=True)
        PEXT_N_OCs = PEXT_N[~PEXT_N['OC Posición'].isna()].copy()
        PEXT_N_OCs['REQ'] = PEXT_N_OCs['REQ'].astype(str)
        
        for col in PEXT_N_OCs.columns:
            PEXT_N_OCs[col] = PEXT_N_OCs[col].astype(str)
        
        agg_func = {col: 'first' for col in PEXT_N_OCs.columns if col != 'OC Posición'}
        # Agrupar por 'Asignatura' y aplicar la función de agregación
        PEXT_agg = PEXT_N_OCs.groupby('OC Posición').agg(agg_func).reset_index()
        return PEXT_agg
    except: 
        return None
        pass 
        
def addSite_V2(EA_act_df: pd.DataFrame, SAP_4_use: pd.DataFrame) -> pd.DataFrame:
    """Función que toma las tablas SAP y extrae los sites y los códigos de AHI.
       Devuelve un DataFrame con la información añadida."""
    
    # Merge y eliminación de duplicados
    df_merged = pd.merge(EA_act_df, SAP_4_use, on='CONCATENADO', how='left')
    df_merged = df_merged.drop_duplicates(subset='CONCATENADO')
    
    # Convertir 'PEP Desc' a string
    df_merged['PEP Desc'] = df_merged['PEP Desc'].astype(str)
    
    # Precompilar la expresión regular
    pattern = re.compile(r'(?<=\d)(?=[a-zA-Z])')
    
    # Función para dividir 'PEP Desc'
    def split_pep_desc(x):
        if pattern.search(x):
            return re.split(pattern, x)
        else:
            return [np.nan, x]
        

def add_info_SITE(PEA_EI:pd.DataFrame,PAP_DB:pd.DataFrame,PAP2M:pd.DataFrame,report_old:bool):
    continuar = True
    if not reporte_old:
        directorio = r'\\LIMBIPBICOV01.claro.pe\Red Región Norte\EAS\Tablas SAP'
        while continuar:
            SAP_4_use = (load_merge(directorio)
                          .pipe(Ac.convert_columns_to_str, ['OC','Pos']) # Uso pipe para poder encadenar funciones, dentro de pipe puedo pasar cualquier funcion a ejecutar en el df con sus argumentos
                          .assign(CONCATENADO=lambda df: df["OC"].str.cat(df["Pos"], sep=""))
                          .loc[:, ['CONCATENADO','PEP Desc','Fecha OC']]
                          .replace({'PEP Desc': {'TJ5125-SANTIAGO DE CHUCO' : 'TJ5125-SANTIAGO_DE_CHUCO'}})
                          .dropna(subset=['PEP Desc']))
            #Separo por el numero inicial pues aveces hay solpes
            EA_4 = PEA_EI[PEA_EI.CONCATENADO.astype(str).str.startswith('4')]
            EA_2 = PEA_EI[~PEA_EI.CONCATENADO.astype(str).str.startswith('4')]
            EA_SAP = pd.concat([addSite_V2(EA_4.astype(str), SAP_4_use).drop(columns=['PEP Desc']), EA_2], axis=0) # Añado el sitio y concateno del SAP
            #EA_SAP = clean_nan(EA_SAP,'SITE') # Limpio vacios
            EA_SAP_SITE = pd.merge(EA_SAP,PAP_DB,on='ID_SITIO',how='left') ## Añado site del datab del PAP
            EA_SAP_SITE = combine_and_rename(EA_SAP_SITE,[('SITE_x', 'SITE_y')],{'SITE_x':'SITE'}) 

            EA_SAP_PAP = (pd.merge(EA_SAP_SITE, PAP2M, how='left', on='OC Posición')
                          .pipe(update_and_rename, [('SITE_x', 'SITE_y')], {'SITE_x': 'SITE'})
                          .rename(columns={'DOC_PREC': 'NUMERO OC'}))
            # Convertir fechas y limpiar filas duplicadas
            EA_SAP_PAP['F.Creación'] = pd.to_datetime(EA_SAP_PAP['F.Creación'], format="%d/%m/%Y %I:%M:%S %p")

            
            
            a,b = get_OCS(EA_SAP_SITE) # obtiene ocs mas frecuentes con sitios vacios
            print(f"Aun faltan descargar: {a}")
            respuesta = input("¿Quieres continuar? (s/n): ")
            if respuesta.lower() != 's':
                continuar = False
        #display(EA_SAP_SITE.info())
        
        EA_SAP_PAP.to_csv(r'D:\Scripts1\Code\ActPEA\CODE\Temps\Base_EAS',index=False)
        print("Actualizo info de las Tablas SAP")
        return EA_SAP_PAP
    else:
        print("El reporte es OLD, uso la base")
        EA_SAP_load = pd.read_csv(r'D:\Scripts1\Code\ActPEA\CODE\Temps\Base_EAS') # literalmete uso la base 
        return EA_SAP_load


def add_PEXT_info(EA_SAP_PAP:pd.DataFrame,PEXT_agg:pd.DataFrame):
    try:
        EA_PAP_SAP_PEXT = pd.merge(EA_SAP_PAP,PEXT_agg,on='OC Posición',how='left')
        EA_PAP_SAP_PEXT = combine_and_rename(EA_PAP_SAP_PEXT,[('SITE_x', 'SITE_y')],{'SITE_x':'SITE'})
    except: 
        EA_PAP_SAP_PEXT = EA_SAP_PAP    
    return EA_PAP_SAP_PEXT

def add_RLL_info(EA_PAP_SAP_PEXT:pd.DataFrame,OT_agg_ID_NM_PRO_RLL:pd.DataFrame,OT_agg_ID_PRO_RLL:pd.DataFrame):
    ########### ROLL OUT ###################################### 
    # Merge a 3 columnas
    EA_PAP_PEXT_OTS_3 = (pd.merge(EA_PAP_SAP_PEXT, OT_agg_ID_NM_PRO_RLL, on=['ID_SITIO', 'SITE', 'PROVEEDOR'], how='left')
                         .assign(ESTATUS_GENERAL=lambda x: x['ESTATUS GENERAL'].combine_first(x['Status OT']))
                         .drop(columns=['Status OT']))
    EA_PAP_PEXT_OTS_3.drop(columns=['ESTATUS GENERAL'],inplace=True)
    # Separar por los que tienen OT(Hcieron merge) y el resto(no hicieron merge)
    EA_PAP_PEXT_OTS_3_R = EA_PAP_PEXT_OTS_3[~EA_PAP_PEXT_OTS_3.OT.isna()]
    EA_PAP_PEXT_OTS_3_NR = (EA_PAP_PEXT_OTS_3[EA_PAP_PEXT_OTS_3.OT.isna()]
                            .drop(columns=['OT', 'Proyecto']))
    
    
    # Merge a 2 columnas
    EA_PAP_PEXT_OTS_2 = (pd.merge(EA_PAP_PEXT_OTS_3_NR, OT_agg_ID_PRO_RLL, on=['ID_SITIO', 'PROVEEDOR'], how='left')
                         .pipe(combine_and_rename, [('SITE_x', 'SITE_y'), ('ESTATUS_GENERAL', 'Status OT')], {'SITE_x': 'SITE'}))
    # Unir los dos merges
    EA_PAP_SAP_PEXT_RLL = pd.concat([EA_PAP_PEXT_OTS_2, EA_PAP_PEXT_OTS_3_R], axis=0)
    
    # Resetear índice
    EA_PAP_SAP_PEXT_RLL = EA_PAP_SAP_PEXT_RLL.reset_index(drop=True)
    
    # Reemplazar valores en la columna 'Proyecto'
    EA_PAP_SAP_PEXT_RLL['Proyecto'].replace({
        'STREET CELL/NUEVO RADIOBASE': 'STREET CELL',
        'NUEVO RADIOBASE': 'JHORDAN',
        'STREET CELL': 'DEMETRIO'}, inplace=True)
    
    # Renombrar columna y combinar valores
    EA_PAP_SAP_PEXT_RLL = (EA_PAP_SAP_PEXT_RLL
                           .rename(columns={'RESPONSABLE DE IMPLEMENTACION': 'RESPONSABLE_DE_EA'})
                           .assign(RESPONSABLE_DE_EA=lambda x: x['RESPONSABLE_DE_EA'].combine_first(x['Proyecto']))
                           .drop(columns=['Proyecto']))
    return EA_PAP_SAP_PEXT_RLL

def add_MOD_info(EA_PAP_SAP_PEXT_RLL:pd.DataFrame,OT_agg_ID_N_PRO_M:pd.DataFrame,OT_agg_ID_PRO_M:pd.DataFrame):
    ################### MODERNIZACION ################################################
    # Realizar merge inicial en 3 columnas
    EA_PAP_SAP_PEXT_RLL_M3 = (pd.merge(EA_PAP_SAP_PEXT_RLL, OT_agg_ID_N_PRO_M, on=['ID_SITIO', 'SITE', 'PROVEEDOR'], how='left')
                              .pipe(combine_and_rename, [('RESPONSABLE_DE_EA_x', 'RESPONSABLE_DE_EA_y'),
                                                        ('OT_x', 'OT_y'), ('ESTATUS_GENERAL', 'Status OT')],
                                    {'RESPONSABLE_DE_EA_x': 'RESPONSABLE_DE_EA', 'OT_x': 'OT'}))
    
    # Separar filas con y sin OT
    EA_PAP_SAP_PEXT_RLL_M_R = EA_PAP_SAP_PEXT_RLL_M3[~EA_PAP_SAP_PEXT_RLL_M3.OT.isna()]
    EA_PAP_SAP_PEXT_RLL_M_NA = (EA_PAP_SAP_PEXT_RLL_M3[EA_PAP_SAP_PEXT_RLL_M3.OT.isna()]
                                .drop(columns=['OT']))
    
    # Realizar merge en 2 columnas
    EA_PAP_SAP_PEXT_RLL_M2 = (pd.merge(EA_PAP_SAP_PEXT_RLL_M_NA, OT_agg_ID_PRO_M, on=['ID_SITIO', 'PROVEEDOR'], how='left')
                              .pipe(combine_and_rename, [('RESPONSABLE_DE_EA_x', 'RESPONSABLE_DE_EA_y'),
                                                        ('SITE_x', 'SITE_y'), ('ESTATUS_GENERAL', 'Status OT')],
                                    {'RESPONSABLE_DE_EA_x': 'RESPONSABLE_DE_EA', 'OT_x': 'OT', 'SITE_x': 'SITE'}))
    
    # Unir los resultados de los merges
    EA_PAP_SAP_PEXT_RLL_M = pd.concat([EA_PAP_SAP_PEXT_RLL_M_R, EA_PAP_SAP_PEXT_RLL_M2], axis=0)
    
    ########################## ENERGIA #############################333
    EA_PAP_SAP_PEXT_OTS = (pd.merge(EA_PAP_SAP_PEXT_RLL_M,OT_cut_E_ID_PRO,on=['ID_SITIO','PROVEEDOR'],how='left')
                           .pipe(combine_and_rename, [('RESPONSABLE_DE_EA_x','RESPONSABLE_DE_EA_y'),
                                                     ('SITE_x','SITE_y'),('ESTATUS_GENERAL','Status OT'),('OT_x','OT_y')],
                                                    {'RESPONSABLE_DE_EA_x':'RESPONSABLE_DE_EA','OT_x':'OT','SITE_x':'SITE'}))
    return EA_PAP_SAP_PEXT_OTS

def CleanColumns(EA_PAP_SAP_PEXT_OTS:pd.DataFrame):
    # Filtrar filas con 'ESTATUS' igual a 'EN PROCESO'
    EA_PAP_SAP_PEXT_OTS = EA_PAP_SAP_PEXT_OTS[EA_PAP_SAP_PEXT_OTS.ESTATUS == 'EN PROCESO']
    
    # Eliminar columnas completamente vacías
    EA_PAP_SAP_PEXT_OTS = EA_PAP_SAP_PEXT_OTS.dropna(axis=1, how='all')
    
    # Eliminar columnas innecesarias
    cols_to_drop = ['F.Creación', 'REQ', 'ANTIGUAMIENTO_PAP', 'NUM_SOT', 'NATURALEZA', 'OC Posición',
                    'T.CAMBIO', 'AÑO', 'ESTATUS', 'PPTO_INICIAL', 'PPTO_FINAL', 'RECLA_INTERNA', 'ID',
                    'RECIBIDO_USD', 'MONEDA_ORIG', 'CONTRATISTA', 'COMPROMETIDO', 'RECIBIDO', 'EN_PROCESO',
                    'TIPO_PRES', 'TIPO_PRY']
    EA_PAP_SAP_PEXT_OTS = EA_PAP_SAP_PEXT_OTS.drop(columns=cols_to_drop)
    
    # Rellenar valores nulos en 'ESTADO_PAP' y 'PROVEEDOR'
    EA_PAP_SAP_PEXT_OTS['ESTADO_PAP'] = EA_PAP_SAP_PEXT_OTS['ESTADO_PAP'].fillna('Sin Registrar')
    EA_PAP_SAP_PEXT_OTS['PROVEEDOR'] = EA_PAP_SAP_PEXT_OTS['PROVEEDOR'].fillna('Sin Proveedor')
    # Limpiar IDs y convertir 'CONCATENADO' a int64
    EA_PAP_SAP_PEXT_OTS = limpiar_id(EA_PAP_SAP_PEXT_OTS, 'ID_SITIO', 'SITE')
    EA_PAP_SAP_PEXT_OTS['CONCATENADO'] = EA_PAP_SAP_PEXT_OTS['CONCATENADO'].astype('int64')
    # Copiar DataFrame y mapear nombres de analistas
    EA_act = EA_PAP_SAP_PEXT_OTS.copy()
    analistas_map = {'JENNY PIZAN': 'JENNY', 'DANNER YARLEQUE': 'DANNER', 'LAURA RAFAEL': 'LAURA'}
    # Convertir nombres de analistas a mayúsculas y reemplazar según el mapa
    EA_act['RESPONSABLE_DE_EA'] = EA_act['RESPONSABLE_DE_EA'].str.upper().replace(analistas_map)
    return EA_act

def Valid_Total(EA_PAP_SAP_PEXT_OTS:pd.DataFrame,Cash_In:float):
    EA_PAP_SAP_PEXT_OTS['EN_PROC_USD'] = EA_PAP_SAP_PEXT_OTS['EN_PROC_USD'].astype(float)
    EA_PAP_SAP_PEXT_OTS = EA_PAP_SAP_PEXT_OTS.drop_duplicates(subset='CONCATENADO')
    cash_out = EA_PAP_SAP_PEXT_OTS['EN_PROC_USD'].sum()
    print("La diferencia de montos es: ",Cash_In - cash_out)


def add_responsablebyML(EA_act:pd.DataFrame,modelo,selector,max_error:int):
    EA_act_2_ML = EA_act.copy()
    EA_act_NN = EA_act[~EA_act['RESPONSABLE_DE_EA'].isna()] # Extraigo las que no son vacias 
    df4Model =  process_2_model(EA_act_2_ML,selector) ## Normalizo las columnas como matriz densa
    predict_array = modelo.predict(df4Model) ## Hago la prediccion de todo el DF 
    df_pred = pd.DataFrame(predict_array, columns = ['RESPONSABLE_PRED']) # Lo convierto en un df unidimensional para poder comparar
    # Un dataframe de la columna predicha por el modelo
    
    # Filtrar predicciones para filas no nulas y nulas
    df_pred2Comp = df_pred.loc[EA_act_NN.index].copy()
    df_pred2fill = df_pred.drop(EA_act_NN.index).copy()
    blank_rows = len(df_pred2fill)
    
    if blank_rows > 0:    ## Compruebo si hay filas sin asignar responsable              
        print(f"Se llenaran {blank_rows} filas.))")
        diferencias = EA_act_NN['RESPONSABLE_DE_EA'].compare(df_pred2Comp['RESPONSABLE_PRED']) #Comparo filas no vacias del total
        print(len(diferencias)) # imprimero el numero de filas diferentes 
        const_error = round(len(diferencias)/len(EA_act_NN) * 100,2) ## Metrica del error del modelo.
        if const_error < max_error: # Si el error esta debajo del umbral
            df_pred2fill.rename(columns={'RESPONSABLE_PRED':'RESPONSABLE_DE_EA'},inplace=True)
            EA_act_2_ML['RESPONSABLE_DE_EA'] = EA_act_2_ML['RESPONSABLE_DE_EA'].combine_first(df_pred2fill['RESPONSABLE_DE_EA'])#Relleno las finlas que no esten vacias
            print(f"El error es de {const_error}. \nSe etiquetaron {len(df_pred2fill)} filas de responsable por ML")
            return EA_act_2_ML
        else: 
            return print("El modelo necesita ser reentrenado") 
    else: print("Data completa , no se usa el modelo")
    return EA_act_2_ML
    

def Load_old_EA(path):
    # Obtener el DataFrame reciente y renombrar columna
    df_analist, _ = get_recent_df(path, sheet_name='Sheet1')
    df_analist.rename(columns={'RESPONSABLE DE EA': 'RESPONSABLE_DE_EA','ESTATUS_GENERAL_x':'ESTATUS_GENERAL'}, inplace=True)    
    return df_analist

def CreateExcel(EA_act:pd.DataFrame,df_analist:pd.DataFrame,reporte_old,model,selector):
    if not reporte_old:

        # Seleccionar columnas relevantes
        df_analist_cut = df_analist[['CONCATENADO', 'SITE', 'Estado de EA', 'RESPONSABLE_DE_EA', 'ESTATUS_GENERAL']].copy()
        
        # Heredar información del DataFrame anterior
        EA_ACT_A_D = (pd.merge(EA_act, df_analist_cut, on='CONCATENADO', how='left')
                     .pipe(combine_and_rename, [('SITE_x', 'SITE_y')], {'SITE_x': 'SITE'})
                      .pipe(update_and_rename, [('RESPONSABLE_DE_EA_x', 'RESPONSABLE_DE_EA_y'), 
                                                ('ESTATUS_GENERAL_x', 'ESTATUS_GENERAL_y')],
                            {'RESPONSABLE_DE_EA_x': 'RESPONSABLE_DE_EA', 'ESTATUS_GENERAL_x': 'ESTATUS_GENERAL'}))
        
        # Etiquetar con Machine Learning
        print(len(EA_ACT_A_D[EA_ACT_A_D.RESPONSABLE_DE_EA.isna()]))
        EA_ACT_A_D = add_responsablebyML(EA_ACT_A_D, model,selector, 10) ## Añado responsable de EA con ML
        
        # Definir columnas inútiles para eliminar
        useless_columns = ['PEP', 'TIPO DE MATERIAL', 'SUB_DIRECCION', 'Estrategia de Liberación', 'MATERIAL',
                           'Cod.Solicitante', 'TIPO DE MATERIAL', 'TIPO_DOC', 'TIPO_PROYECTOS', 'COD_PROVEEDOR',
                           'POS_PRESUP', 'COMPROM_USD', 'CE_GESTOR', 'RUBRO', 'CENTRO_GESTOR', 'Proyecto CAPEX', 
                           'Cod.Solicitante', 'Proyecto CAPEX', 'AÑO', 'PPTO_INICIAL', 'RECLA_INTERNA', 'PPTO_FINAL', 
                           'Estrategia de Liberación', 'MONEDA_ORIG', 'POS_PRESUP']
        
        # Rellenar valores nulos en 'Estado de EA'
        EA_ACT_A_D['Estado de EA'].fillna('PENDIENTE', inplace=True)
        
        # Crear nuevo archivo editable
        print("Se crea nuevo archivo editable")
        Ac.Excel_format(EA_ACT_A_D, fr'C:\Users\C26764\America Movil Peru S.A.C\EAS - 1\EAUPDATE{Today_D_M}.xlsx', useless_columns) ## Ruta en la que se crea el Excel
    else:
        print("No se crea archivo editable")
        pass

    

def load_OCS(path):
    with open(r'D:\Scripts1\Code\ActPEA\CODE\Temps\DF_OCS.pkl', "rb") as archivo:
        Ocs_DF = pickle.load(archivo)
    # Eliminar duplicados y convertir a lista
    return Ocs_DF


def Add_Date_E(df_analist:pd.DataFrame,Ocs_DF:list[str],df_yesterday):
    # Obtener DataFrame más reciente del archivo CSV
    # Merge para comparar estados
    Ocs_F = Ocs_DF.drop_duplicates()['CONCATENADO'].tolist()

    df_analist_Y = pd.merge(df_analist, df_yesterday[['CONCATENADO', 'Fecha de Ejecucion']], on='CONCATENADO', how='left')
    
    # Seleccionar columnas relevantes y reemplazar valores
    df_analist_cut = df_analist_Y[['CONCATENADO', 'SITE', 'Estado de EA', 'RESPONSABLE_DE_EA', 'ESTATUS_GENERAL', 'Fecha de Ejecucion']].copy()
    df_analist_cut['ESTATUS_GENERAL'].replace('TERMINADO', 'EJECUTADO', inplace=True)
    # Filtrar filas donde el estado general ha cambiado a 'EJECUTADO' y no están en la lista de OCs etiquetadas
    df_row_diff = df_analist_cut[df_analist_cut['ESTATUS_GENERAL'].ne(df_yesterday['ESTATUS_GENERAL']) & 
                                 (df_analist_cut['ESTATUS_GENERAL'] == 'EJECUTADO') & 
                                 (~df_analist_cut['CONCATENADO'].isin(Ocs_F))]
    
    # Asignar fecha de ejecución a las filas que cambiaron a 'EJECUTADO'
    df_row_diff['Fecha de Ejecucion'] = pd.to_datetime(datetime.date.today().strftime('%d-%m-%Y'), format='%d-%m-%Y')
    
    
    Ocs_DF = pd.concat([Ocs_DF,df_row_diff[['CONCATENADO','Fecha de Ejecucion']]],axis=0)
    
    with open(r'D:\Scripts1\Code\ActPEA\CODE\Temps\DF_OCS.pkl', "wb") as archivo:
        pickle.dump(Ocs_DF, archivo)
    df_analist_cut.update(df_row_diff)# Las filas diferentes etiquetadas actualizan el df de analista
    df_analist_cut.rename(columns={'ESTATUS GENERAL':'ESTATUS_GENERAL'},inplace=True)    
    df_analist_cut['Fecha de Ejecucion'] = pd.to_datetime(df_analist_cut['Fecha de Ejecucion'], unit='ns')

    return df_analist_cut 

def update_data_from_excel(EA_act:pd.DataFrame,df_analist_cut:pd.DataFrame):
    # Mapa para abreviar nombres de analistas
    analistas_map = {'JENNY PIZAN': 'JENNY', 'DANNER YARLEQUE': 'DANNER', 'LAURA RAFAEL': 'LAURA'}
    
    # Merge y procesamiento de datos
    EA_ACT_A_D = (
        pd.merge(EA_act, df_analist_cut, on='CONCATENADO', how='left')
        .pipe(combine_and_rename, [('SITE_x', 'SITE_y')], {'SITE_x': 'SITE'})
        .pipe(update_and_rename, [('RESPONSABLE_DE_EA_x', 'RESPONSABLE_DE_EA_y'), ('ESTATUS_GENERAL_x', 'ESTATUS_GENERAL_y')],
              {'RESPONSABLE_DE_EA_x': 'RESPONSABLE_DE_EA', 'ESTATUS_GENERAL_x': 'ESTATUS_GENERAL'})
    )
    
    # Convertir nombres de analistas a mayúsculas y reemplazar según el mapa
    EA_ACT_A_D['RESPONSABLE_DE_EA'] = EA_ACT_A_D['RESPONSABLE_DE_EA'].str.upper().replace(analistas_map)
    
    # Normalizar valores y rellenar vacíos
    EA_ACT_A_D['RESPONSABLE_DE_EA'] = EA_ACT_A_D['RESPONSABLE_DE_EA'].fillna('Por asignar')
    EA_ACT_A_D['Estado de EA'] = EA_ACT_A_D['Estado de EA'].fillna('PENDIENTE').str.upper()
    return EA_ACT_A_D


def norma_data(EA_ACT_A_D:pd.DataFrame):
    # Definir y aplicar valores específicos en 'Estado de EA'
    valores_anuladas = ['OC ANULADA']
    valores_anular = ['PENDIENTE ANULAR']
    EA_ACT_A_D['Estado de EA'] = EA_ACT_A_D['Estado de EA'].replace(valores_anuladas, 'ANULADA').replace(valores_anular, 'ANULAR')
    
    # Asignar valor por defecto si no está en los valores permitidos
    valores_permitidos = ['PENDIENTE', 'EJECUTADO', 'ANULAR', 'ANULADA', 'LIQUIDADO']
    EA_ACT_A_D['Estado de EA'] = EA_ACT_A_D['Estado de EA'].where(EA_ACT_A_D['Estado de EA'].isin(valores_permitidos), 'PENDIENTE')
    EA_ACT_A_D['Estado de EA'].replace('EJECUTADA', 'Ea ejecutada', inplace=True)
    
    # Crear columna de mes para agrupar y convertir a fecha
    EA_ACT_A_D['FECHA_DOC'] = pd.to_datetime(EA_ACT_A_D['FECHA_DOC'].str.replace(' 00:00:00', ''), format='%Y-%m-%d')
    
    # Convertir columnas a tipo int64
    EA_ACT_A_D[['NUMERO OC', 'POS_PREC']] = EA_ACT_A_D[['NUMERO OC', 'POS_PREC']].astype('int64')
    return EA_ACT_A_D


def update_EstadoEA(EA_ACT_A_D:pd.DataFrame,EAs_2Merge:pd.DataFrame):
    df_merged = pd.merge(EA_ACT_A_D, EAs_2Merge, on=['NUMERO OC', 'POS_PREC'], how='left')
    df_merged['Estado de EA'].update(df_merged['Estado EA'])
    df_merged['Fecha de Ejecucion'].update(df_merged['F.Aprob'])
    # Eliminar columnas innecesarias y asignar el DataFrame final
    EA_ACT_A_D = df_merged.drop(columns=['Estado EA', 'F.Aprob'])
    EA_ACT_A_D['ESTATUS_GENERAL'].replace(pd.NA,'Sin detalle',inplace=True)
    EA_ACT_A_D['Estado de EA'] = EA_ACT_A_D['Estado de EA'].str.title()    
    return EA_ACT_A_D

import datetime
def save_time_s(EA_ACT_A_D:pd.DataFrame,Today_str:str):
     ## guardo el TSLM
    filename = r'\\LIMBIPBICOV01.claro.pe\Red Región Norte\EAS\last_run.json'
    # Carga la última fecha de ejecución
    last_run_date = Ac.load_last_run_date(filename)
    
    # Comprueba si la celda ya se ha ejecutado hoy
    if last_run_date != datetime.datetime.now().date():
        # Tu código aquí
        print('Ejecutado: ',Today_str)
        #process_to_bcsv(PRE_all_act,'D:/Prepa/TIME.S/Prepa_TS1.csv',Today_str)
        EA_ACT_A_D.to_csv(fr'D:\EA\Resultados\EAS_{Today_str}.csv',index=False) ## guardo el TODAY (Opcional) 
        process_to_bcsv(EA_ACT_A_D.copy(),r'\\LIMBIPBICOV01.claro.pe\Red Región Norte\EAS\EA_TS1.csv',Today_str)# añado al acumulado
    
        # Guarda la fecha de hoy como la última fecha de ejecución
        Ac.save_last_run_date(filename)
    else:
        print("El código ya se ha ejecutado hoy.")   

def standar_columns(df:pd.DataFrame,columns:list[str]):
    try:
        df1 = df[columns].copy()
    except KeyError as e:
        # Identificamos las columnas faltantes
        missing_columns = list(set(columns) - set(df.columns))
        # Rellenamos las columnas faltantes con valores vacíos
        for col in missing_columns:
            df.loc[:,col] = ''
        # Seleccionamos las columnas nuevamente
        df1 = df[columns].copy()   
    return df1  

def save2Bi(EA_ACT_A_D:pd.DataFrame):
    # Definimos las columnas
    Columnas_2_use = ['NUMERO OC', 'POS_PREC', 'FECHA_DOC', 'SOLICITANTE', 'TEXTO', 'PROVEEDOR',
                      'EN_PROC_USD', 'CE_GESTOR', 'TIPO_PROYECTOS']
    last_columns = ['CLASIF_FINANZAS', 'CLASIF_RED_1', 'CLASIF_RED_2', 'ID_SITIO', 'SITE',
                    'PAP', 'ESTADO_PAP', 'RESPONSABLE_DE_EA', 'ESTATUS_GENERAL', 'OT',
                    'Estado de EA', 'Fecha de Ejecucion']
    total_columns = Columnas_2_use + last_columns
    
    # Intentamos seleccionar las columnas deseadas
    
    EA_ACT_A_B = standar_columns(EA_ACT_A_D,total_columns)
    
        
    list_columns = ['TEXTO' ,'SITE','ESTATUS_GENERAL','TIPO_PROYECTOS']
    for column in list_columns:
        EA_ACT_A_B.loc[:,column] = EA_ACT_A_B[column].str.capitalize()
    EA_ACT_A_B.rename(columns={'ESTATUS_GENERAL':'ESTATUS GENERAL'},inplace=True)
    EA_ACT_A_B.to_csv(r'\\LIMBIPBICOV01.claro.pe\Red Región Norte\EAS\EAs.csv',index=False) 
    subprocess.call(["python",r"D:\Scripts1\Code\ActPEA\CODE\2BI_Norma.py"])



# # Cargar archivos 

# In[3]:


def load_model(modelo_path: str):
    with open(modelo_path, "rb") as f:
        return pickle.load(f)


# In[4]:


df_yesterday, _ = get_recent_csv(r'D:\EA\Resultados')


# In[5]:


Ocs_DF = load_OCS(r'D:\Scripts1\Code\ActPEA\CODE\Temps\DF_OCS.pkl')


# In[6]:


df_analyst = Load_old_EA(r'C:\Users\C26764\America Movil Peru S.A.C\EAS - 1')


# In[7]:


PAP,PAP_DB = get_PAP_data(r'D:\Scripts1\Code\ActPEA\archvis\PAP\Administrativo',r'D:\Scripts1\Code\ActPEA\archvis\PAP')


# In[8]:


PAP.info()


# In[8]:


PEA,Path_data = get_recent_df_by_N(r'C:\Users\C26764\America Movil Peru S.A.C\EAS - 2',sheet_name= 'DATA',# Ruta carpeta de EA Base
                                              prefijo_nombre_archivo='Pendiente de entrada al') 
from datetime import datetime ## Calculo la aniguedad del Reporte 
reporte_old = getime_of_report(Path_data)
reporte_old = check_run(Path_data)


# In[9]:


OT_cut,OT = load_OTs(r'D:\Scripts1\Code\ActPEA\archvis\OTs')


# In[9]:


EAs,_= get_recent_df(r'D:\Scripts1\Code\ActPEA\archvis\EAS','Hoja2')


# In[10]:


EAs.info()


# In[13]:


EAs[EAs.Posición.isna()]


# In[11]:


def Prep_EAs(EAs:pd.DataFrame):
    EAs = EAs.dropna(subset='Posición')
    EAs_2Merge = (EAs.drop(columns=['IMPUTACION'])
                  .query("GERENCIA != 'Proyectos OyM'") #Filtro columnas
                  .rename(columns={'OC':'NUMERO OC', 'Posición': 'POS_PREC'})
                  .astype({'POS_PREC': 'int64', 'NUMERO OC': 'int64'}) #Cambio el tipo de dato
                  [['NUMERO OC', 'POS_PREC', 'Estado EA', 'F.Aprob']] # Filtro estas columnas 
                  .replace({'Estado EA': {'AF ejecutada':'Ea ejecutada',
                                          'Aprobado': 'Ea ejecutada', 
                                          'Observado Soporte': 'Observado', 
                                          'En Registro': 'PENDIENTE'}})) # Reemplazo valores
    # Configurar localización y convertir fechas
    locale.setlocale(locale.LC_ALL, '')
    EAs_2Merge['F.Aprob'] = pd.to_datetime(EAs_2Merge['F.Aprob'], format="%d/%m/%Y %I:%M:%S %p").dt.date
    locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')    
    return EAs_2Merge


# # Preproces 

# In[12]:


EAs_2Merge = Prep_EAs(EAs)


# In[19]:


OT_agg_ID_PRO_RLL,OT_agg_ID_NM_PRO_RLL,OT_agg_ID_PRO_M,OT_agg_ID_N_PRO_M,OT_cut_E_ID_PRO = PrepOts(OT_cut,OT)


# In[20]:


PEA_EI,Cash_In = PrepPEA(PEA)


# In[21]:


PEXT_agg = PrepPext(r'D:\Scripts1\Code\ActPEA\archvis\PEXT_S')


# In[22]:


def PrePAP(PAP:pd.DataFrame):
    # Seleccionar columnas y realizar merge
    locale.setlocale(locale.LC_ALL, '')
    PAP_N_S = Process_PAP(PAP).query("`ESTADO_PAP` != 'Rechazado'")
    PAP_N_S.drop_duplicates(subset='OC Posición',inplace=True)
    PAP2M = PAP_N_S[['PAP', 'ESTADO_PAP', 'SITE', 'ANTIGUAMIENTO_PAP', 'OC Posición', 'F.Creación']]
    return PAP2M


# In[23]:


PAP2M = PrePAP(PAP)


# # Proces MAIN

# In[24]:


EA_SAP_load = add_info_SITE(PEA_EI,PAP_DB,PAP2M,reporte_old)


# In[25]:


EA_SAP_load.info()


# In[26]:


EA_PAP_SAP_PEXT = add_PEXT_info(EA_SAP_load,PEXT_agg)


# In[27]:


EA_PAP_SAP_PEXT_RLL = add_RLL_info(EA_PAP_SAP_PEXT,OT_agg_ID_NM_PRO_RLL,OT_agg_ID_PRO_RLL)


# In[28]:


EA_PAP_SAP_PEXT_OTS = add_MOD_info(EA_PAP_SAP_PEXT_RLL,OT_agg_ID_N_PRO_M,OT_agg_ID_PRO_M)


# In[29]:


EA_PAP_SAP_PEXT_OTS.info()


# In[30]:


EA_act = CleanColumns(EA_PAP_SAP_PEXT_OTS)


# In[31]:


Valid_Total(EA_act,Cash_In)


# In[32]:


CreateExcel(EA_act,df_analyst,reporte_old,modelo_v2,selector) ## EXCEL


# In[33]:


df_analyst = Load_old_EA(r'C:\Users\C26764\America Movil Peru S.A.C\EAS - 1')


# In[34]:


import datetime
df_analist_cut = Add_Date_E(df_analyst,Ocs_DF,df_yesterday)


# In[35]:


EA_ACT_A_D = update_data_from_excel(EA_act,df_analist_cut)
EA_ACT_A_D_N = norma_data(EA_ACT_A_D)


# In[36]:


EA_ACT_A_P= update_EstadoEA(EA_ACT_A_D_N,EAs_2Merge)


# In[37]:


EA_ACT_A_P['Estado de EA'].value_counts()


# In[ ]:





# In[38]:


EA_ACT_A_P[EA_ACT_A_P['Estado de EA'] == 'Ea Ejecutada']['EN_PROC_USD'].sum()


# In[39]:


EA_ACT_A_P['EN_PROC_USD'].sum()


# # Load DATA 

# In[40]:


save_time_s(EA_ACT_A_P,Today_str)


# In[41]:


save2Bi(EA_ACT_A_P)


# In[ ]:





# In[ ]:





# In[ ]:




