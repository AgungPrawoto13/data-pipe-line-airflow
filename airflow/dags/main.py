import api
import jangkar
import numpy as np
import pandas as pd
import geopandas as gpd
import re, string

from tqdm import tqdm
from datetime import date
from shapely.wkt import loads
from dotenv import dotenv_values
from shapely.geometry import Point
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from PreProcessRelawan import PreProcessRelawan

env = dotenv_values(".env")

pre_jangkar = jangkar.PreProcessJangkar(env)
pre_api = api.APIGet(env)

data_api = pre_api.GetData()
data_jangkar = pre_jangkar.main_jangkar()

## Fungsi untuk menghasilkan titik acak dalam satu polygon
def Random_Points_in_Polygon(polygon, number):
    points = []
    minx, miny, maxx, maxy = polygon.bounds
    while len(points) < number:
        pnt = Point(np.random.uniform(minx, maxx), np.random.uniform(miny, maxy))
        if polygon.contains(pnt):
            points.append(pnt)
    return points

## Fungsi untuk mendapatkan titik acak dalam satu polygon berdasarkan kolom 'Alamat 1'
def get_random_point(row):
    polygon = row['geometry']
    number_of_points = 1  # Sesuaikan dengan kebutuhan
    return Random_Points_in_Polygon(polygon, number_of_points)[0]

# Fungsi untuk mendapatkan nilai Lng dan Lat dari geometri
def extract_coordinates(geometry_str):
    geom = loads(geometry_str)
    if geom.geom_type == 'Point':
        return geom.x, geom.y
    elif geom.geom_type == 'MultiPolygon':
        # Mendapatkan koordinat tengah dari MultiPolygon (contoh sederhana)
        return geom.centroid.x, geom.centroid.y
    else:
        return None, None

# Alamat Blank
## Provinsi
def blank_address_province(data):

    if(data.empty):
        return pd.DataFrame()

    # Menggabungkan data dengan shapefile provinsi berdasarkan kolom 'Provinsi'
    merged_prov = data.merge(spatial_data_provinsi, left_on='Provinsi', right_on='WADMPR')

    # Menambahkan kolom baru 'Random_Point' yang berisi titik acak dalam polygon
    merged_prov['Random_Point'] = merged_prov.apply(get_random_point, axis=1)

    merged_prov

    merged_prov['Random_Point']=merged_prov['Random_Point'].astype(str)

    # Membuat kolom "Lng" dan "Lat"
    merged_prov['Lng'], merged_prov['Lat'] = zip(*merged_prov['Random_Point'].map(extract_coordinates))

    # Rename kolom
    merged_prov.rename(columns ={'Provinsi':'Prov', 'KDPPUM':'Kode_Prov_Kemendagri','WADMPR':'Provinsi','geometry':'geometry_prov','Random_Point':'geometry'},inplace=True)
    merged_prov

    merged_prov['Kelurahan'] = None
    merged_prov['Kecamatan'] = None
    merged_prov['Kabupaten'] = None
    merged_prov['Kode_Kab_Kemendagri']=None
    merged_prov['Kode_Kec_Kemendagri']=None
    merged_prov['Kode_Kel_Kemendagri']=None
    merged_prov['Geocode'] = 'Random'
    data_prov = merged_prov [['index',
                'Tanggal',
                'Prov',
                'Kab/Kota',
                'Capres-Cawapres',
                'Kegiatan',
                'Isu',
                'Nama Tokoh',
                'Posisi',
                'Nama Organisasi',
                'Kategori',
                'Scope',
                'Nama Media',
                'Jenis Media',
                'Alamat 1',
                'Alamat 2',
                'Alamat 3',
                'Link/sumber',
                'Lat',
                'Lng',
                'geometry','Kode_Prov_Kemendagri', 'Kode_Kab_Kemendagri',
                'Kode_Kec_Kemendagri',
                'Kode_Kel_Kemendagri','Provinsi','Kabupaten','Kecamatan','Kelurahan', 'Geocode', 'Sumber Data']]
    
    print("Pastikan Jumlah Data Ini sama degan yang di Sheet alamat_blank_pro!")
    print("Total: ", len(data_prov))

    return data_prov

## Kabupaten   
def blank_address_kab(data):
    # Membaca DataFrame (gantilah ini dengan cara Anda membaca DataFrame)
    #data = pd.read_excel(env.get('ROOT_FOLDER') + "Update Data "+ yesterday + ".xlsx", sheet_name = 'alamat_blank_kab')
    if data.empty:
        return pd.DataFrame()

    # Menggabungkan data dengan shapefile provinsi berdasarkan kolom 'Provinsi'
    merged_kab = data.merge(spatial_data_kab, left_on='Kab/Kota', right_on='WADMKK')

    # Menambahkan kolom baru 'Random_Point' yang berisi titik acak dalam polygon
    merged_kab['Random_Point'] = merged_kab.apply(get_random_point, axis=1)

    merged_kab

    merged_kab['Random_Point']=merged_kab['Random_Point'].astype(str)

    # Membuat kolom "Lng" dan "Lat"
    merged_kab['Lng'], merged_kab['Lat'] = zip(*merged_kab['Random_Point'].map(extract_coordinates))

    merged_kab.rename(columns ={'Index':'index','Provinsi':'Prov', 'KDPPUM':'Kode_Prov_Kemendagri','KDPKAB':'Kode_Kab_Kemendagri','WADMPR':'Provinsi','WADMKK':'Kabupaten','geometry':'geometry_prov','Random_Point':'geometry'},inplace=True)
    merged_kab

    # Menambahkan 5 kolom baru dengan nilai awal kosong (NaN)
    merged_kab['Kelurahan'] = None
    merged_kab['Kecamatan'] = None
    merged_kab['Kode_Kec_Kemendagri']=None
    merged_kab['Kode_Kel_Kemendagri']=None
    merged_kab['Geocode']='Random'

    data_kab = merged_kab [['index',
            'Tanggal',
            'Prov',
            'Kab/Kota',
            'Capres-Cawapres',
            'Kegiatan',
            'Isu',
            'Nama Tokoh',
            'Posisi',
            'Nama Organisasi',
            'Kategori',
            'Scope',
            'Nama Media',
            'Jenis Media',
            'Alamat 1',
            'Alamat 2',
            'Alamat 3',
            'Link/sumber',
            'Lat',
            'Lng',
            'geometry','Kode_Prov_Kemendagri', 'Kode_Kab_Kemendagri',
            'Kode_Kec_Kemendagri',
            'Kode_Kel_Kemendagri','Provinsi','Kabupaten','Kecamatan','Kelurahan', 'Geocode', 'Sumber Data']]
    data_kab

    print("Pastikan Jumlah Data Ini sama degan yang di Sheet alamat_blank_kab!")
    print("Total: ", len(data_kab))

    # data_kab.to_excel(env.get('ROOT_FOLDER') + "Result/hasil_lanjutan_"+ str(yesterday) +"_nan_kab.xlsx",index=False)

    return data_kab

def alamat_lengkap(data_non_nan):
    # Baca file raw data

    # data_non_nan= pd.read_excel(env.get('ROOT_FOLDER') + "Update Data "+ yesterday + ".xlsx", sheet_name = 'alamat_lengkap')

    if data_non_nan.empty:
        return pd.DataFrame()
    
    # Pemecahan kolom 'Kota' menjadi dua kolom 'Kota1' dan 'Kota2' dengan delimiter koma
    data_non_nan[['Lat', 'Lng']] = data_non_nan['Koordinat 1'].str.split(',', expand=True)
    data_non_nan

    data_non_nan.rename(columns = {'Index':'index'},inplace=True)
    data_non_nan

    # String Match

    data_non_nan['Lng']= data_non_nan['Lng'].astype(float)
    data_non_nan['Lat']= data_non_nan['Lat'].astype(float)
    data_non_nan['geometry'] = data_non_nan.apply(lambda row: Point(row['Lng'], row['Lat']), axis=1)
    data_non_nan.rename(columns={'Provinsi': 'Prov'}, inplace=True)
    
    data_non_nan['Kelurahan'] = None
    data_non_nan['Kecamatan'] = None
    data_non_nan['Kabupaten'] = None
    data_non_nan['Provinsi'] = None
    data_non_nan['Kode_Prov_Kemendagri']=None
    data_non_nan['Kode_Kab_Kemendagri']=None
    data_non_nan['Kode_Kec_Kemendagri']=None
    data_non_nan['Kode_Kel_Kemendagri']=None
    data_non_nan['Geocode']='Aktual'
    

    for index, row in data_non_nan.iterrows():
        point = row['geometry']
        matching_polygon = spatial_data_kelurahan[spatial_data_kelurahan.contains(point)]
        
        if not matching_polygon.empty:
            data_non_nan.loc[index,"Kelurahan"] = matching_polygon['WADMKD'].values[0]
            data_non_nan.loc[index,"Kecamatan"] = matching_polygon['WADMKC'].values[0]
            data_non_nan.loc[index,"Kabupaten"] = matching_polygon['WADMKK'].values[0]
            data_non_nan.loc[index,"Provinsi"] = matching_polygon['WADMPR'].values[0]
            data_non_nan.loc[index,"Kode_Prov_Kemendagri"] = matching_polygon['KDPPUM'].values[0]
            data_non_nan.loc[index,"Kode_Kab_Kemendagri"] = matching_polygon['KDPKAB'].values[0]
            data_non_nan.loc[index,"Kode_Kec_Kemendagri"] = matching_polygon['KDCPUM'].values[0]
            data_non_nan.loc[index,"Kode_Kel_Kemendagri"] = matching_polygon['KDEPUM'].values[0]

    data_non_nan = data_non_nan [['index',
        'Tanggal',
        'Prov',
        'Kab/Kota',
        'Capres-Cawapres',
        'Kegiatan',
        'Isu',
        'Nama Tokoh',
        'Posisi',
        'Nama Organisasi',
        'Kategori',
        'Scope',
        'Nama Media',
        'Jenis Media',
        'Alamat 1',
        'Alamat 2',
        'Alamat 3',
        'Link/sumber',
        'Lat',
        'Lng',
        'geometry','Kode_Prov_Kemendagri', 'Kode_Kab_Kemendagri',
        'Kode_Kec_Kemendagri',
        'Kode_Kel_Kemendagri','Provinsi','Kabupaten','Kecamatan','Kelurahan','Geocode', 'Sumber Data']]
    
    print("Pastikan Jumlah Data Ini sama degan yang di Sheet alamat_lengkap!")
    print("Total: ", len(data_non_nan))

    # data_non_nan.to_excel(env.get('ROOT_FOLDER') + "Result/hasil_lanjutan_"+  str(yesterday) +"_non_nan.xlsx",index=False)
    return data_non_nan

def remove_tweet_special(text):
    # remove tab, new line, ans back slice
    text =  str(text)
    text = text.replace('\\t'," ").replace('\\n'," ").replace('\\u'," ").replace('\\',"")
    # remove non ASCII (emoticon, chinese word, .etc)
    text = text.encode('ascii', 'replace').decode('ascii')
    # remove mention, link, hashtag
    text = ' '.join(re.sub(r"([@#][A-Za-z0-9]+)|(\w+:\/\/\S+)"," ", text).split())
    text = text.translate(str.maketrans("","",string.punctuation))
    # remove incomplete URL
    return text.replace("http://", " ").replace("https://", " ")


def connect_to_db():
    try:
        engine = create_engine(
            f"mysql+mysqlconnector://{env.get('CONFIG_DB_USERNAME')}:{env.get('CONFIG_DB_PASSWORD')}@{env.get('CONFIG_DB_HOST')}/{env.get('CONFIG_DB_NAME')}"
        )
        conn = engine.connect()

        if conn:
            print("Connected to MySQL Database!")
            return conn

    except OperationalError as e:
        print("Oppss... Something Went Wrong When Connecting to the Database!")
        print("Error Connect Database:", str(e))

    return None

def insert_data_to_db(conn, df):
    trans = conn.begin()
    try:
        print("Sample Data to be inserted:")

        df = pd.concat([df, data_jangkar, data_api]).reset_index(drop=True)
        df.to_excel(env.get('ROOT_FOLDER') + 'Result/' + str(date.today()) + ' Result Updated Data With Out Prov.xlsx', index=False)
        
        df['geometry'] = df['geometry'].astype(str)
        columns_to_exclude = ['prov', 'kab/kota', 'alamat 2', 'alamat 3']
        df.drop(columns=columns_to_exclude, inplace=True)

        df['tanggal'] = pd.to_datetime(df['tanggal'])
        print(df.tail()) 
        
        # df.to_sql("040409_2024_kinetik_relawan",
        #           conn,
        #           if_exists='append',
        #           chunksize=1000,
        #           index=False,
        #           dtype={
        #               "index": sqlalchemy.types.NVARCHAR(length=60),
        #               "Kode_Prov_Kemendagri": sqlalchemy.types.NVARCHAR(length=60),
        #               "Kode_Kab_Kemendagri": sqlalchemy.types.NVARCHAR(length=60),
        #               "Kode_Kec_Kemendagri": sqlalchemy.types.NVARCHAR(length=60),
        #               "Kode_Kel_Kemendagri": sqlalchemy.types.NVARCHAR(length=60)
        #           })

        trans.commit()
        print("Data Inserted Successfully into the Database!")
        
    except Exception as e:
        trans.rollback()
        print(f"Error Database: {e}")

    finally:
        conn.close()


def main():
    
    # Receive data from main.py
    df_full_address, df_blank_kab, df_blank_pro = PreProcessRelawan()
    if df_full_address.empty & df_blank_kab.empty & df_blank_pro.empty:
        print("Check Manualy Your Data!")
    else:
        global spatial_data_provinsi, spatial_data_kab, spatial_data_kelurahan
        # File paths
        file_paths = [
            env.get('ROOT_FOLDER') + "Polygon/Polygon Indonesia - 1 Provinsi 3/Polygon Indonesia - 1 Provinsi.shp",
            env.get('ROOT_FOLDER') + "Polygon/Polygon Indonesia - 2 Kabupaten-Kota 1 - 0.5/Polygon Indonesia - 2 Kabupaten-Kota.shp",
            env.get('ROOT_FOLDER') + "Polygon/Polygon_Kelurahan.json"
        ]

        spatial_data = []

        # Read each file with tqdm progress bar
        for file_path in tqdm(file_paths, desc='Reading Polygon files'):
            spatial_data.append(gpd.read_file(file_path))

        # Assign the data to specific variables
        spatial_data_provinsi, spatial_data_kab, spatial_data_kelurahan = spatial_data

        blankPro = blank_address_province(df_blank_pro)
        blankKab = blank_address_kab(df_blank_kab)
        fullAddress = alamat_lengkap(df_full_address)

        result = pd.concat([fullAddress, blankKab, blankPro]).reset_index(drop=True)
        print("Gabungan PErtama")

        # Delete 'Kota Administrasi'
        if not result.empty:
            result['Kab/Kota'] = result['Kab/Kota'].str.replace('Kota Administrasi', '', regex=True).str.strip()
            result['Kabupaten'] = result['Kabupaten'].str.replace('Kota Administrasi', '', regex=True).str.strip()

        result['Alamat 1'] = result['Alamat 1'].apply(lambda x: remove_tweet_special(x) if pd.notnull(x) else '')
        # result['Nama Media'] = result['Nama Media'].apply(lambda x: remove_tweet_special(x) if pd.notnull(x) else '')
        # result['Nama Tokoh'] = result['Nama Tokoh'].apply(lambda x: remove_tweet_special(x) if pd.notnull(x) else '')
        # result['Capres-Cawapres'] = result['Capres-Cawapres'].apply(lambda x: remove_tweet_special(x) if pd.notnull(x) else '')
        # result['Nama Organisasi'] = result['Nama Organisasi'].apply(lambda x: remove_tweet_special(x) if pd.notnull(x) else '')
       
        lowercase_columns = [column.lower() for column in result.columns.tolist()]
        result.columns = lowercase_columns

        result.rename(columns = {'capres-cawapres':'capres_cawapres','nama tokoh': 'nama_tokoh','nama organisasi':'nama_organisasi', 'nama media' : 'nama_media', 
                                'jenis media': 'jenis_media', 'lng': 'lang', 'alamat 1': 'alamat', 'link/sumber': 'link_sumber', 'geocode': 'geocord', 
                                'sumber data': 'sumber_data', 'kode_prov_kemendagri': 'kode_provinsi_kemendagri_2022', 'kode_kab_kemendagri': 'kode_kabupaten_kemendagri_2022',
                                'kode_kec_kemendagri' : 'kode_kecamatan_kemendagri_2022', 'kode_kel_kemendagri': 'kode_kelurahan_kemendagri_2022'}, inplace=True)
         
        result.to_excel(env.get('ROOT_FOLDER') + 'Result/' + str(date.today()) + ' Result Updated Data With Prov.xlsx', index=False)
        
        # Update in Database and One Drive
        connection = connect_to_db()

        if connection:
            insert_data_to_db(connection, result)
        else:
            print("Error Connection to Database!")

    # except Exception as e:
    #     print(f'Error Huft: {e}')

if __name__ == "__main__":
    main()

