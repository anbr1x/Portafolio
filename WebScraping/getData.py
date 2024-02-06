from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import time
import pandas as pd
from selenium.common.exceptions import ElementClickInterceptedException,TimeoutException,StaleElementReferenceException
from selenium.webdriver.support import expected_conditions as EC
import traceback
from datetime import date
import glob
import os


class Extract_Data(list):
    def __init__(self,list):
        self.data_dict = {
            list[0]: [],
            list[1]: [],
            list[2]: [],
            list[3]: [],
            list[4]: [],
            list[5]: [],
            list[6]: [],
            list[7]: [],
            list[8]: []
        }
        self.nameC = []
         

    def Extraccion_de_data(self,driver):
        wait = WebDriverWait(driver, 6)
        
        ########## Panel General de Datos ########################################################
        wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@id="panel-detail"]/div')))
        print("Panel listo,itero para extraer la data")


        
        for clave, valor in self.data_dict.items():
            catch_data(driver,clave,valor)
            
        print('Se extrajo la data de: ',self.data_dict["Departamento / Provincia / Distrito"][-1])  
        
    def to_df(self):
        # Crear el dataframe
        df = pd.DataFrame(self.data_dict)
        df.columns = self.nameC
        return df 

    def get_name(self,driver):
        if len(self.nameC) < 9:
            wait = WebDriverWait(driver, 6)
            wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@id="panel-detail"]/div')))        
            for clave in self.data_dict.keys():
                element = driver.find_element(By.XPATH, '//*[@id="panel-detail"]/div/table[1]/tbody/tr[td[contains(text(),"{}")]]//td[1]'.format(clave))
                name = element.text
                self.nameC.append(name)
        

    
        
def catch_data(driver,data_name,array):
     """ obtengo driver_element, su data y lo agrego al diccionario de listas"""
     element = driver.find_element(By.XPATH, '//*[@id="panel-detail"]/div/table[1]/tbody/tr[td[contains(text(),"{}")]]//td[2]'.format(data_name))
     data = element.text
     array.append(data)
     #print(data_name,data)
      


def get_district_name(Distrito_element):
    Nombre_de_Distrito = Distrito_element.find_element(By.CSS_SELECTOR, 'a') 
    return Nombre_de_Distrito.get_attribute('value')

def select_district(Distrito_element, wait):
    Distrito_bttn = wait.until(EC.element_to_be_clickable((By.XPATH, ('//*[@id="btn-dist"]'))))   
    Distrito_bttn.click()
    Distrito_element.click()

def wait_for_elements_to_disappear(driver,time=5):
    WebDriverWait(driver, time).until(EC.invisibility_of_element_located((By.XPATH, '/html/body/div[2]/div/div/div/div/div')))
    WebDriverWait(driver, time).until(EC.invisibility_of_element_located((By.XPATH, "//div[@class='loading-page']//p[@class='website-down parpadea']")))
    WebDriverWait(driver, time).until(EC.invisibility_of_element_located((By.CLASS_NAME, 'align-middle')))
    WebDriverWait(driver, time).until(EC.invisibility_of_element_located((By.CLASS_NAME, 'modal-backdrop fade')))
    WebDriverWait(driver, time).until(EC.invisibility_of_element_located((By.XPATH,'/html/body/div[2]/div/div/div/div/div/p[1]/i')))

def wait_for_elements_to_appear(wait):
    wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="panel-spline"]/div[1]/ul')))
    wait.until(EC.presence_of_element_located((By.XPATH,'//*[@id="panel-detail"]/div')))

def get_district_string(driver, wait=10):
    wait = WebDriverWait(driver, wait)
    wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@id="panel-detail"]/div/table[1]/tbody/tr[2]/td[2]')))
    Place_element = driver.find_elements(By.XPATH, '//*[@id="panel-detail"]/div/table[1]/tbody/tr[2]/td[2]')
    Place_data = Place_element[0].text
    L_place = Place_data.split('/')
    return L_place[-1].strip().replace('Ã‘',"Ñ")

def close_panel(wait):
    for i in range(3):
        try:
            bar_bttn = wait.until(EC.element_to_be_clickable((By.XPATH, ('//*[@id="panel-detail"]/span'))))
            bar_bttn.click()
            break
        except TimeoutException:
            if i < intentos - 1:  # i es base 0, por lo que restamos 1
                continue
            else:
                raise

def DataofDist (Extractor, Distrito_element, Dists_of_df, driver,RETRIES=5,WAIT_TIME=7):
    wait = WebDriverWait(driver, WAIT_TIME)
    Dist_element_name = get_district_name(Distrito_element)
    
    if Dist_element_name in Dists_of_df: 
        try:
            select_district(Distrito_element,wait)
            WebDriverWait(driver, 7).until(EC.invisibility_of_element_located((By.XPATH, '/html/body/div[2]/div/div/div/div/div')))
            print('Click en: ',Dist_element_name)
            wait_for_elements_to_disappear(driver)
            wait_for_elements_to_appear(wait)
            ####################### Here intercambio el nombre de las keys de los disccionarios ###################
            Extractor.get_name(driver)
            
            for i in range(RETRIES): #Intento hasta que coincidan los nombres de los distritos que quiero extraer
                    Dist_str = get_district_string(driver)
                    if Dist_element_name == Dist_str:
                        lat_bttn = wait.until(EC.element_to_be_clickable((By.XPATH,'//*[@id="panel-spline"]/span')))
                        wait_for_elements_to_disappear(driver)
                        lat_bttn.click()
                        print('Luego de: ',i,'Intentos, coincidencia en: ',Dist_str)
                        Extractor.Extraccion_de_data(driver)
                        close_panel(wait)
                        break
                    else : 
                        time.sleep(1)
        except (ElementClickInterceptedException, TimeoutException) as e:
            out = wait.until(EC.element_to_be_clickable((By.XPATH,('/html/body/div[2]'))))
            print(f"Se produjo una excepción: {type(e).__name__}")
            print(str(e))
            print(traceback.format_exc())
            out.click()
            print('Salto la Excepcion en: ',Dist_element_name) 


        #close_panel(wait)




def select_button(driver,geo_clas,lugar):
        wait = WebDriverWait(driver, 5)
        #WebDriverWait(driver, 6).until(EC.invisibility_of_element_located((By.XPATH, '/html/body/div[2]/div/div/div/div/div')))
        bttn_dsply = wait.until(EC.element_to_be_clickable((By.XPATH, (f'//*[@id="btn-{geo_clas}"]')))) 
        wait.until(EC.invisibility_of_element_located((By.CLASS_NAME, 'align-middle')))
        wait.until(EC.invisibility_of_element_located((By.CLASS_NAME, 'modal-backdrop fade')))
        bttn_dsply.click()
        bttn_plc = wait.until(EC.presence_of_element_located((By.XPATH, f'//li/a[@id="{geo_clas}" and @value="{lugar}"]')))
        #print(len(bttn_plc.text))
        bttn_plc.click()     




def login(driver,user_ID,password_ID, user_NAME = "C26764", password = 'Elbrix<314'):
    user = driver.find_element(By.ID, user_ID)
    psswrd = driver.find_element(By.ID, password_ID)
    user.clear()
    user.send_keys(user_NAME)
    psswrd.clear()
    psswrd.send_keys(password) 
    psswrd.send_keys(Keys.RETURN)

def DicofDf(directorio):
    dfs = {}
    for filename in os.listdir(directorio):
        file_path = os.path.join(directorio, filename)
        # Verifica si el path es un archivo y no un directorio
        if os.path.isfile(file_path):
            df = pd.read_csv(file_path)  
            # Obtiene el nombre del archivo sin la extensión
            nombre_sin_extension = os.path.splitext(filename)[0]
            # Añade el DataFrame al diccionario
            dfs[nombre_sin_extension] = df

    return dfs

def NormDf (DF_scrap): 
    Correguir = {
    'NEPEÃ‘A': 'NEPEÑA',
    'SAÃ‘A': 'SAÑA',
    'FERREÃ‘AFE': 'FERREÑAFE',
    'CAÃ‘ARIS': 'CAÑARIS',
    'CHANCAYBAÃ‘OS': 'CHANCAYBAÑOS',
    'ENCAÃ‘ADA': 'ENCAÑADA',
    'LOS BAÃ‘OS DEL INCA' : 'LOS BAÑOS DEL INCA',
    'PARIÃ‘AS' : 'PARIÑAS'}
    
    DF_scrap = DF_scrap.copy() 
    DF_scrap['Departamento'] = DF_scrap['Departamento'].replace(Correguir)
    DF_scrap['Departamento'] = DF_scrap['Departamento'].str.strip()
    DF_scrap['Provincia'] = DF_scrap['Provincia'].replace(Correguir)
    DF_scrap['Distrito'] = DF_scrap['Distrito'].replace(Correguir)
    DF_scrap['Provincia'] = DF_scrap['Provincia'].str.strip()
    DF_scrap['Distrito'] = DF_scrap['Distrito'].str.strip()
    return DF_scrap

def CalcDiff(dfs,lista_of_dfs):
    Df_Now = dfs[lista_of_dfs[0]]
    Df_Old = dfs[lista_of_dfs[1]]
    O = Df_Old[['Departamento','Provincia','Distrito','Users','Market Share Claro FB %']]
    N = Df_Now[['Departamento','Provincia','Distrito','Users','Market Share Claro FB %']]
    merged_df = pd.merge(N,O,how='left',on=['Departamento','Provincia','Distrito'])
    merged_df['Diff'] = merged_df['Users_x'] - merged_df['Users_y']
    merged_df['Diff_MS'] = merged_df['Market Share Claro FB %_x'] - merged_df['Market Share Claro FB %_y']
    merged_df['Diff'] = merged_df['Diff'].fillna(0)
    print('La diferencia de usuarios a nivel del Norte es: ',merged_df['Diff'].sum())
    print('La diferencia de usuarios a nivel del Norte es: ',merged_df['Diff'].sum())
    Df_Now = Df_Now.join(merged_df['Diff'], how='left')
    Df_Now = Df_Now.join(merged_df['Diff'], how='left')
    Df_Now = Df_Now.rename(columns = {'Diff' : 'DIFF_users'})
    
    Df_Now = Df_Now.drop(columns=['Transporte','Capacidad'])
    return Df_Now

def CalcDiffV2(dfs : pd.DataFrame, list_of_dfs: list[str], Colums2diff: list[str] ):
    Df_Now = dfs[list_of_dfs[0]]
    Df_Old = dfs[list_of_dfs[1]]
    O = Df_Old[['Departamento','Provincia','Distrito']+Colums2diff] 
    N = Df_Now[['Departamento','Provincia','Distrito']+Colums2diff]
    merged_df = pd.merge(N,O,how='left',on=['Departamento','Provincia','Distrito'])
    for colum in Colums2diff:
        merged_df['Diff_'+colum] = merged_df[colum+'_x'] - merged_df[colum+'_y']
        merged_df['Diff_'+colum].fillna(0)
        print(f'La diferencia de {colum} a nivel del Norte es: ',merged_df['Diff_'+colum].mean())
        Df_Now = Df_Now.join(merged_df['Diff_'+colum], how='left')
    
    return Df_Now
        
        
    
    



    
    