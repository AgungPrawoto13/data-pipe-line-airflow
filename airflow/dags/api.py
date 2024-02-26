import os
import db
import requests
import pandas as pd
import pre_process_rbpr

from PreProcessRelawan import generateDateYesterdayeFormat
from PreProcessRelawan import get_geocode
from datetime import datetime, timedelta
from requests.auth import HTTPBasicAuth 

class APIGet:
    
    def __init__(self, env):
        self.env = env
        self.url = env.get("CONFIG_API_URL")
        self.pre_rbpr = pre_process_rbpr.PreProcessRBPR(env)
        self.db_main = self.db_main = db.DataBase(env.get('CONFIG_DB_USERNAME'), env.get('CONFIG_DB_PASSWORD'), env.get('CONFIG_DB_HOST'), env.get('CONFIG_DB_NAME'))
        self.api_df = None
    
    def convertTime(self, time):
        timestamp_seconds = time / 1000
        datetime_utc = datetime.utcfromtimestamp(timestamp_seconds)
        datetime_gmt = datetime_utc + timedelta(hours=7)
        time = datetime_gmt.strftime("%Y-%m-%d %H:%M:%S")

        return time
    
    def make_request(self, tipe, time):
        try:
            password = self.env.get('CONFIG_API_PASS')
            response = requests.get(self.url, params={'page':1, 'size': 1000, 'entitas_tipe':f'{tipe}', 'waktu_sesudah':f'{time}'}, auth = HTTPBasicAuth(self.env.get('CONFIG_API_USER'), password=password))
            if response.status_code == 200:
                return response.json()['data']['data']
            else:
                print("Respon 400")
        
        except requests.exceptions.RequestException as e:
            print(f"Error making request: {e}")
            return None
    
    def adjustRequirement(self, data):
        new_df = pd.DataFrame()
        pemetaan_kategori = {
            'Deklarsi': 'Deklarasi',
            'Dukungan': 'Deklarasi',
            'Sembako': 'Kegitan Sosial',
            'Bazar': 'Kegitan Sosial',
            'Adakan': 'Aktivitas Relawan',
            'Senam': 'Aktivitas Relawan',
            'Jalan Santai': 'Aktivitas Relawan',
            'Dzikir': 'Kegiatan Keagamaan',
            'Fun': 'Aktivitas Relawan',
            'Futsal': 'Aktivitas Relawan',
            'Pelatihan': 'Pemberdayaan Masyarakat',
            'Doa': 'Kegiatan Keagamaan'
        }

        new_df['tanggal'] = data['waktu_sesudah']
        new_df['prov'] = data['lokasi_provinsi']
        new_df['kab/kota'] = data['lokasi_kabupaten_kota']
        new_df['capres_cawapres'] = data['entitas_arah_dukungan']
        new_df['kegiatan'] = data['agenda_aktivitas']
        new_df['nama_tokoh'] = data['entitas_tokoh_internal']
        new_df['nama_organisasi'] = data['entitas_nama']
        new_df['alamat'] = data['lokasi_spesifik']
        new_df['Koordinat 1'] = data['lokasi_spesifik'].apply(lambda x: get_geocode(x) if pd.notnull(x) else '')
        new_df['sumber_data'] = "Team Kemang"
        new_df['created_at'] = data['inserted_at']
        new_df[['kategori','file']] = None
        new_df = self.pre_rbpr.checkKabKotProv(new_df, 'Koordinat 1', 0, 'Team Kemang')
        new_df['geometry'] = new_df['geometry'].astype(str)
        
        for kata_kunci, nilai_baru in pemetaan_kategori.items():
            if new_df['kategori'].notna().any():
                new_df.loc[new_df['kegiatan'].str.contains(kata_kunci), 'kategori'] = nilai_baru.title().strip()
        
        return new_df

    def GetData(self):
        time = '2024-01-01 00:00:00|2024-02-30 00:00:00'

        data_relawan = self.make_request('Relawan', time)

        data_api = pd.DataFrame(data_relawan)
        data_api['inserted_at'] = data_api['inserted_at'].apply(self.convertTime)
        data_db = self.db_main.get_data('040409_2024_kinetik_relawan','Team Kemang')
        self.api_df = data_api[~data_api['inserted_at'].isin(data_db['created_at'])]
        
        if not self.api_df.empty:
            self.api_df = self.adjustRequirement(self.api_df)
            self.api_df = self.pre_rbpr.generateUuid(self.api_df, "Team Kemang")
            
            self.api_df[self.pre_rbpr.kolom_urutan].sort_values(by='tanggal').to_excel(f"{self.env.get('ROOT_FOLDER')}Kemang/" + generateDateYesterdayeFormat() + " Update Data.xlsx", index=False)
            return self.api_df[self.pre_rbpr.kolom_urutan]
        else:
            print("Belum ada data update team Kemang")
            pd.DataFrame(columns=self.pre_rbpr.kolom_urutan)