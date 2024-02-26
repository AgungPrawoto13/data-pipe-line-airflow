import db
import pandas as pd
import pre_process_rbpr
import uuid

from tqdm import tqdm
from PreProcessRelawan import get_geocode
from shapely.geometry import Point
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

class PreProcessJangkar():

    def __init__(self, env):
        self.env = env 
        self.db_jangkar = db.DataBase(self.env.get('CONFIG_DB_USERNAME_JANGKAR'), self.env.get('CONFIG_DB_PASSWORD_JANGKAR'), self.env.get('CONFIG_DB_HOST_JANGKAR'), self.env.get('CONFIG_DB_NAME_JANGKAR'))
        self.db_main = db.DataBase(self.env.get('CONFIG_DB_USERNAME'), self.env.get('CONFIG_DB_PASSWORD'), self.env.get('CONFIG_DB_HOST'), self.env.get('CONFIG_DB_NAME'))
        self.pre_rbpr = pre_process_rbpr.PreProcessRBPR(env)
        self.kode_kel = pd.read_excel(f"{self.env.get('ROOT_FOLDER')}Polygon/wil_kel_bps.xlsx")

    def generateUuid(self,df):
        print("generate uuid")
        
        uuid_list = set()
        for _, row in df.iterrows():
            while True:
                my_uuid = str(uuid.uuid4().int)[-10:]
                # tanggal = pd.to_datetime(row['tanggal']).strftime('%d%m%Y')
                tqdm.pandas()
                month = pd.to_datetime(row['tanggal'], format='%Y-%m-%d %H:%M:%S').strftime('%m')
                tgl = pd.to_datetime(row['tanggal'], format='%Y-%m-%d %H:%M:%S')
                week_number_of_month = (tgl.day - 1) // 7 + 1
                formatted_week_number = str(week_number_of_month).zfill(2)
                generate_uuid = "5" + str(month) + formatted_week_number + my_uuid

                if generate_uuid not in uuid_list:
                    uuid_list.add(generate_uuid)
                    break

        df['index'] = list(uuid_list)  
        df['index'] = df['index'].astype(str)
        return df

    def running_query(self,query, conn):
        print("get data")
        
        result = conn.execute(text(query))
        df = pd.DataFrame(result, columns=result.keys())
        df.rename(columns={'Tanggal':'tanggal','Capres_Cawapres':'capres_cawapres','Kegiatan':'kegiatan'}, inplace=True)
        df['sumber_data'] = 'Team Jangkar'
        df[['file','created_at']] = None

        return df

    def main_jangkar(self):

        #kordinat_null
        # data = pd.read_excel(f"{self.env.get('ROOT_FOLDER')}Jangkar/kordinat_null.xlsx")
        # df[['isu','scope','nama_media','jenis_media','link_sumber','posisi','kode_provinsi_kemendagri_2022','kode_kabupaten_kemendagri_2022','kode_kelurahan_kemendagri_2022',
        #     'kode_kecamatan_kemendagri_2022','provinsi','kabupaten','kecamatan','kelurahan','geocord']] = None
        # data = self.pre_rbpr.checkKabKotProv(data, 'Koordinat 1', 0, "Team RBPR")
        # data.to_excel(f"{self.env.get('ROOT_FOLDER')}Jangkar/Team RBPR.xlsx")

        query = self.db_jangkar.make_query()
        connection = self.db_jangkar.connect_to_db()
        df = self.running_query(query, connection)
        df_db = self.db_main.get_data('040409_2024_kinetik_relawan','Team Jangkar')
        df = df[~df['tanggal'].isin(df_db['tanggal'])]
        df.loc[df['id_laporan_tag'] == 7, ['kategori']] = 'Deklarasi'
        tqdm.pandas(desc='Progress Get Provinsi In Google Maps')

        df['Koordinat 1'] = df['alamat'].progress_apply(lambda x: get_geocode(x) if pd.notnull(x) else '')
        df = self.pre_rbpr.checkKabKotProv(df,'Koordinat 1', 0,"Team Jangkar")
        df['prov'] = df['provinsi']
        df['kab/kota'] = df['kabupaten']
        df = self.generateUuid(df)

        return df[self.pre_rbpr.kolom_urutan]
