import pandas as pd
#####################################################################
##df2 actuliza df1
def update_values(df1, df2, index_name, columns): 
    idx = index_name

    def update(row, column):
        if row[idx] in df2[idx].values:
            idx_value = row[idx]
            if pd.isna(df2.loc[df2[idx] == idx_value, column].values[0]):
                return row[column]
            else:
                return df2.loc[df2[idx] == idx_value, column].values[0]
        else:
            return row[column]

    for column in columns:
        df1.loc[:, column] = df1.apply(lambda row: update(row, column), axis=1)
    return df1
    
def convert_columns_to_str(df, columns):
    for column in columns:
        df[column] = df[column].astype(str)
    return df
    
def merge_dataframes(df1, df2, merge_on, index):
    merged_df = pd.merge(df1, df2, on=merge_on, how='left')
    merged_df.set_index(index, inplace=True)
    return merged_df
    
def date_format(df, columnas):
    for col in columnas:
        try:
            df[col] = pd.to_datetime(df[col])
            df[col] = df[col].dt.strftime('%d/%m/%Y')
        except ValueError as e:
            print(f"Error al convertir la columna {col}: {e}")
    return df
def crear_area(df):
        map_Analistas = {
        'ANGGIE': 'Construccion',
        'DEMETRIO' : 'Construccion',
        'EDWIN' : 'Construccion',
        'JHORDAN' :'Construccion',
        'JORGE' : 'Construccion',
        'Marlon' : 'Construccion',
        'JENNY' : 'Implementacion',
        'DANNER' : 'Implementacion',
        'MARCO' : 'Implementacion',
        'LAURA' : 'Implementacion'}
        df['Area'] = df['RESPONSABLE EA'].map(map_Analistas)
        return df 
