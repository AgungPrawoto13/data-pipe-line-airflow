import pandas as pd
import sqlalchemy
import sshtunnel

from sqlalchemy import create_engine
from sshtunnel import SSHTunnelForwarder
from sqlalchemy.exc import OperationalError

class DataBase:

    def __init__(self, user, pas, host, db):
        self.engine = create_engine(f"mysql+mysqlconnector://{user}:{pas}@{host}/{db}")
        self.conn = self.engine.connect()
        self.trans = self.conn.begin()
    
    def connect_to_db(self):
        try:
            if self.conn:
                print("Connected to MySQL Database!")
                return self.conn

        except OperationalError as e:
            print("Oppss... Something Went Wrong When Connecting to the Database!")
            print("Error:", str(e))

        return self.conn
    
    def get_data(self, table, sumber):        
        print("Get Data Database", sumber)

        sql = f'SELECT * FROM {table}'
        df_sql = pd.read_sql(sql, con=self.engine)
        return df_sql[df_sql['sumber_data'] == sumber]
    
    def insert_data_to_db(self, df):
        try:
            print("Sample Data to be inserted:")
            
            df.to_sql("040409_2024_kinetik_relawan",
                        self.conn,
                        if_exists='append',
                        chunksize=1000,
                        index=False,
                        dtype={
                            "index": sqlalchemy.types.NVARCHAR(length=60),
                            "Kode_Prov_Kemendagri": sqlalchemy.types.NVARCHAR(length=60),
                            "Kode_Kab_Kemendagri": sqlalchemy.types.NVARCHAR(length=60),
                            "Kode_Kec_Kemendagri": sqlalchemy.types.NVARCHAR(length=60),
                            "Kode_Kel_Kemendagri": sqlalchemy.types.NVARCHAR(length=60)
                        })

            self.trans.commit()
            print("Data Inserted Successfully into the Database!")
            
        except Exception as e:
            self.trans.rollback()
            print(f"Error Database: {e}")

        finally:
            self.conn.close()
    
    def make_query(self):
        query = """
            SELECT id_laporan_tag, createdAt Tanggal, deskripsi Kegiatan, 'Aktivitas Relawan' as kategori, longitude `Long`, latitude `Lat`, alamat, provinsi Provinsi, kabkot Kabkot, google_provinsi , google_kabkot,  
            CASE
                WHEN id_capres = 1 THEN 'Ganjar Pranowo'
                WHEN id_capres = 2 THEN 'Prabowo Subianto'
                WHEN id_capres = 3 THEN 'Anies Baswedan'
            END Capres_Cawapres,
            org.nama_organisasi,
            org.nama_lengkap nama_tokoh

            FROM laporan l
            left join (	
                SELECT r.id_relawan, o.nama_organisasi, r.nama_lengkap FROM relawan r
                    Inner Join organisasi o
                        ON r.id_organisasi  = o.id_organisasi
                ) org
                
            ON l.id_relawan = org.id_relawan
            WHERE (status_persetujuan, id_laporan_tag) IN 
            (('Diterima', 3),('Diterima', 4),('Diterima', 5),('Diterima', 6),('Diterima', 7),('Diterima', 10));
            """
        return query