import pandas as pd
import geopandas as gpd
import glob
import os, uuid
import nltk, re

from tqdm import tqdm
from PreProcessRelawan import get_geocode
from PreProcessRelawan import generateDateYesterdayeFormat
from nltk.tokenize import word_tokenize
from shapely.geometry import Point

class PreProcessRBPR():

    def __init__(self, env):
        self.file = 'None'
        self.df_rbpr = 'None'
        self.all_rbpr = 'None'
        self.env = env
        self.pol_kel = gpd.read_file(self.env.get('ROOT_FOLDER') + "../Polygon/Polygon_Kelurahan.json")
        self.kolom_urutan = ['index','tanggal','prov','kab/kota', 'capres_cawapres', 'kegiatan', 'isu', 'nama_tokoh','posisi', 'nama_organisasi','kategori', 'scope', 'nama_media','jenis_media', 'alamat','alamat 2','alamat 3', 'link_sumber', 
                        'lat','lang', 'geometry', 'kode_provinsi_kemendagri_2022', 'kode_kabupaten_kemendagri_2022','kode_kecamatan_kemendagri_2022', 'kode_kelurahan_kemendagri_2022', 'provinsi', 'kabupaten','kecamatan',
                        'kelurahan', 'geocord', 'sumber_data','file','created_at']
    
    def generateUuid(self,df, source):
        uuid_list = set()
        for _, row in df.iterrows():
            while True:
                my_uuid = str(uuid.uuid4().int)[-10:]
                month = pd.to_datetime(row['tanggal'], format='%Y-%m-%d %H:%M:%S').strftime('%m')
                tgl = pd.to_datetime(row['tanggal'], format='%Y-%m-%d %H:%M:%S')
                week_number_of_month = (tgl.day - 1) // 7 + 1
                formatted_week_number = str(week_number_of_month).zfill(2)
                
                if source == "Team Kemang":
                    generate_uuid = "1" + str(month) + formatted_week_number + my_uuid
                elif source == "Wijaya":
                    generate_uuid = "2" + str(month) + formatted_week_number + my_uuid
                elif source == "TPN":
                    generate_uuid = "3" + str(month) + formatted_week_number + my_uuid
                elif source == "RBPR":
                    generate_uuid = "4" + str(month) + formatted_week_number + my_uuid
                else:
                    generate_uuid = "5" + str(month) + formatted_week_number + my_uuid
                if generate_uuid not in uuid_list:
                    uuid_list.add(generate_uuid)
                    break

        df['index'] = list(uuid_list)  
        df['index'] = df['index'].astype(str)
        return df
    
    def clean_text(self,teks):
        # Hapus karakter angka
        teks_tanpa_angka = re.sub(r'\d+', '', teks)
        # Hapus tanda baca
        teks_tanpa_tanda_baca = re.sub(r'[^\w\s]', '', teks_tanpa_angka)
        
        return word_tokenize(teks_tanpa_tanda_baca)
    
    def extract_nama_kelurahan(self, address_list):
        for i, word in enumerate(address_list):
            if re.match(r'(DESA|DUSUN|KEL(?:URAHAN)?)$', word, re.IGNORECASE):
                return ' '.join(address_list[i + 1 : i + 3])
        
        return ' '.join(address_list[:2]) if len(address_list) >= 2 else None
    
    def checkKabKotProv(self,data, kordinat, value, source):
        cleaned_df = data
        #blank_kordinat = cleaned_df[cleaned_df['Koordinat 1'] == kordinat]
        blank_kordinat = cleaned_df[cleaned_df[kordinat] == value]
        cleaned_df[['isu','scope','nama_media','jenis_media','link_sumber','posisi','kode_provinsi_kemendagri_2022','kode_kabupaten_kemendagri_2022','kode_kelurahan_kemendagri_2022','kode_kecamatan_kemendagri_2022',
                    'provinsi','kabupaten','kecamatan','kelurahan','geocord','alamat 2','alamat 3']] = None
    
        #melakukan proses koordinat yang bernilai 00
        # print(cleaned_df[cleaned_df['Koordinat 1'] == kordinat])
        if not blank_kordinat.empty:
            print("Process blank Kordinat", source)
            tqdm.pandas(desc=f'Progress Get Provinsi In Google Maps {source}')
            cleaned_df.loc[cleaned_df[kordinat] == value, kordinat] = blank_kordinat['alamat'].progress_apply(lambda x: get_geocode(x) if pd.notnull(x) else '')
        
        print("Process Not Blank Kordinat", source)

        cleaned_df[['lat', 'lang']] = cleaned_df[kordinat].str.split(',', expand=True)
        #cleaned_df[['lat','lang']] = pd.DataFrame(data[kordinat].to_list())
        cleaned_df['lat'] = pd.to_numeric(cleaned_df['lat'], errors='coerce')
        cleaned_df = cleaned_df.dropna(subset=['lat'])
        
        cleaned_df['lang'] = cleaned_df['lang'].replace(',','.', regex=True).astype(float)
        cleaned_df['lat'] = cleaned_df['lat'].replace(',','.', regex=True).astype(float)
        cleaned_df['geometry'] = cleaned_df.apply(lambda row: Point(row['lang'], row['lat']), axis=1)

        for index, row in tqdm(cleaned_df.iterrows(), desc="Process Get Provinsi In Polygon Kelurahan"):
            point = row['geometry']
            matching_polygon = self.pol_kel[self.pol_kel.contains(point)]
            
            if not matching_polygon.empty:
                cleaned_df.loc[index,"kelurahan"] = matching_polygon['WADMKD'].values[0]
                cleaned_df.loc[index,"kecamatan"] = matching_polygon['WADMKC'].values[0]
                cleaned_df.loc[index,"kabupaten"] = matching_polygon['WADMKK'].values[0]
                cleaned_df.loc[index,"provinsi"] = matching_polygon['WADMPR'].values[0]
                cleaned_df.loc[index,"kode_provinsi_kemendagri_2022"] = matching_polygon['KDPPUM'].values[0]
                cleaned_df.loc[index,"kode_kabupaten_kemendagri_2022"] = matching_polygon['KDPKAB'].values[0]
                cleaned_df.loc[index,"kode_kecamatan_kemendagri_2022"] = matching_polygon['KDCPUM'].values[0]
                cleaned_df.loc[index,"kode_kelurahan_kemendagri_2022"] = matching_polygon['KDEPUM'].values[0]
                cleaned_df.loc[index, "geocord"] = "Aktual"
        
        return cleaned_df
    
    def main_rbpr(self):
        self.file = glob.glob(os.path.join(f"{self.env.get('ROOT_FOLDER')}RBPR", "*.xlsx"))
        
        data_list = []
        data_alamat = []

        for f in self.file:
            excel_rbpr = pd.read_excel(f)
            # excel_rbpr = excel_rbpr[excel_rbpr['Status'] == 'publish']
            excel_rbpr.rename(columns = {'Index':'index'},inplace=True)
            excel_rbpr['capres_cawapres'] = 'Ganjar Pranowo'
            excel_rbpr['file'] = f[50:].replace('.xlsx','')
            excel_rbpr.rename(columns={'Group Relawan':'nama_organisasi','Surveyor':'nama_tokoh','Tgl. Survey':'tanggal','Kordinat':'Koordinat 1','Kegiatan':'kegiatan','Kategori':'kategori'}, inplace=True)
            kolom_rbpr = [item for item in excel_rbpr.columns.to_list() if item in ['index','nama_tokoh', 'nama_organisasi','file','tanggal', 'Koordinat 1','kegiatan','kategori', 'capres_cawapres']]
            kolom_alamat = [item for item in excel_rbpr.columns.to_list() if item in ['Alamat/Lokasi','Lokasi Penerima', 'Lokasi Kegiatan','Lokasi','Lokasi Acara','Lokasi Spanduk','Lokasi/Alamat','Alamat Lengkap Relawan','Lokasi RBPR', 'Alamat Rumah','Alamat Lengkap Penerima']]
            if not excel_rbpr[kolom_alamat].empty:
                excel_rbpr['alamat'] = excel_rbpr[kolom_alamat]
                data_list.append(excel_rbpr[kolom_rbpr])
                data_alamat.append(excel_rbpr['alamat'])

        df1 = pd.concat(data_list, axis=0)
        df2 = pd.concat(data_alamat, axis=0)
        self.all_rbpr = pd.concat([df1, df2], axis=1)
        self.all_rbpr['sumber_data'] = 'Team RBPR'
        #self.all_rbpr['tanggal'] = pd.to_datetime(self.all_rbpr['tanggal'], format='%Y-%m-%d h:m:s').dt.strftime('%d/%m/%Y')
        self.all_rbpr = self.generateUuid(self.all_rbpr, "RBPR")
        self.all_rbpr[["geocord","created_at"]] = None

        self.all_rbpr = self.checkKabKotProv(self.all_rbpr,'Koordinat 1','0,0',"Team RBPR")
        self.all_rbpr['prov'] = self.all_rbpr['provinsi']
        self.all_rbpr['kab/kota'] = self.all_rbpr['kabupaten']

        self.all_rbpr[self.kolom_urutan].to_excel(f"{self.env.get('ROOT_FOLDER')}Result/" + generateDateYesterdayeFormat() + "Update Data RBPR.xlsx", index=False)
        
        return self.all_rbpr[self.kolom_urutan]
