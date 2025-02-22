from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
from sqlalchemy import create_engine

spark = SparkSession.builder \
    .appName("ETL_Aquaviario") \
    .config("spark.jars", "/path/to/sqlserver-jdbc.jar") \
    .getOrCreate()

data_path = "/mnt/data/anuario.csv"
data_lake_path = "/mnt/data/datalake/estatisticas_aquaviarias"

sql_server_url = "jdbc:sqlserver://your_server:1433;databaseName=your_db"
sql_server_properties = {
    "user": "your_user",
    "password": "your_password",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

create_tables_sql = """
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'atracacao_fato')
BEGIN
    CREATE TABLE atracacao_fato (
        IDAtracacao INT PRIMARY KEY,
        Tipo_Navegacao_Atracacao VARCHAR(255),
        CDTUP VARCHAR(255),
        Nacionalidade_Armador VARCHAR(255),
        IDBerco INT,
        FlagMCOperacaoAtracacao BIT,
        Berco VARCHAR(255),
        Terminal VARCHAR(255),
        Porto_Atracacao VARCHAR(255),
        Municipio VARCHAR(255),
        Apelido_Instalacao_Portuaria VARCHAR(255),
        UF VARCHAR(2),
        Complexo_Portuario VARCHAR(255),
        SGUF VARCHAR(255),
        Tipo_Autoridade_Portuaria VARCHAR(255),
        Regiao_Geografica VARCHAR(255),
        Data_Atracacao DATE,
        N_Capitania VARCHAR(255),
        Data_Chegada DATE,
        N_IMO VARCHAR(255),
        Data_Desatracacao DATE,
        TEsperaAtracacao FLOAT,
        Data_Inicio_Operacao DATE,
        TEsperaInicioOp FLOAT,
        Data_Termino_Operacao DATE,
        TOperacao FLOAT,
        Ano_Inicio_Operacao INT,
        TEsperaDesatracacao FLOAT,
        Mes_Inicio_Operacao INT,
        TAtracado FLOAT,
        Tipo_Operacao VARCHAR(255),
        TEstadia FLOAT
    );
END;

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'carga_fato')
BEGIN
    CREATE TABLE carga_fato (
        IDCarga INT PRIMARY KEY,
        FlagTransporteViaInterior BIT,
        IDAtracacao INT,
        Percurso_Transporte_Vias_Interiores VARCHAR(255),
        Origem VARCHAR(255),
        Percurso_Transporte_Interiores VARCHAR(255),
        Destino VARCHAR(255),
        STNaturezaCarga VARCHAR(255),
        CDMercadoria VARCHAR(255),
        STSH2 VARCHAR(255),
        Tipo_Operacao_Carga VARCHAR(255),
        STSH4 VARCHAR(255),
        Carga_Geral_Acondicionamento VARCHAR(255),
        Natureza_Carga VARCHAR(255),
        ConteinerEstado VARCHAR(255),
        Sentido VARCHAR(255),
        Tipo_Navegacao VARCHAR(255),
        TEU INT,
        FlagAutorizacao BIT,
        QTCarga INT,
        FlagCabotagem BIT,
        VLPesoCargaBruta DECIMAL(18,2),
        FlagCabotagemMovimentacao BIT,
        Ano_Inicio_Operacao_Atracacao INT,
        FlagConteinerTamanho BIT,
        Mes_Inicio_Operacao_Atracacao INT,
        FlagLongoCurso BIT,
        Porto_Atracacao VARCHAR(255),
        FlagMCOperacaoCarga BIT,
        SGUF VARCHAR(255),
        FlagOffshore BIT,
        Peso_Liquido_Carga DECIMAL(18,2),
        FOREIGN KEY (IDAtracacao) REFERENCES atracacao_fato(IDAtracacao)
    );
END;
"""

engine = create_engine(f"mssql+pymssql://{sql_server_properties['user']}:{sql_server_properties['password']}@your_server/your_db")
with engine.connect() as conn:
    conn.execute(create_tables_sql)

df = spark.read.option("header", True).csv(data_path)

atracacao_fato = df.select(
    col("IDAtracacao").cast("int"),
    col("Tipo de Navegação da Atracação").alias("Tipo_Navegacao_Atracacao"),
    col("CDTUP"),
    col("Nacionalidade do Armador").alias("Nacionalidade_Armador"),
    col("IDBerco").cast("int"),
    col("FlagMCOperacaoAtracacao").cast("boolean"),
    col("Berço").alias("Berco"),
    col("Terminal"),
    col("Porto Atracação").alias("Porto_Atracacao"),
    col("Município").alias("Municipio"),
    col("Apelido Instalação Portuária").alias("Apelido_Instalacao_Portuaria"),
    col("UF"),
    col("Complexo Portuário").alias("Complexo_Portuario"),
    col("SGUF"),
    col("Tipo da Autoridade Portuária").alias("Tipo_Autoridade_Portuaria"),
    col("Região Geográfica").alias("Regiao_Geografica"),
    col("Data Atracação").cast("date").alias("Data_Atracacao"),
    col("N da Capitania").alias("N_Capitania"),
    col("Data Chegada").cast("date").alias("Data_Chegada"),
    col("N do IMO").alias("N_IMO"),
    col("Data Desatracação").cast("date").alias("Data_Desatracacao"),
    col("TEsperaAtracacao").cast("float"),
    col("Data Início Operação").cast("date").alias("Data_Inicio_Operacao"),
    col("TEsperaInicioOp").cast("float"),
    col("Data Término Operação").cast("date").alias("Data_Termino_Operacao"),
    col("TOperacao").cast("float"),
    col("Ano da data de início da operação").cast("int").alias("Ano_Inicio_Operacao"),
    col("TEsperaDesatracacao").cast("float"),
    col("Mês da data de início da operação").cast("int").alias("Mes_Inicio_Operacao"),
    col("TAtracado").cast("float"),
    col("Tipo de Operação").alias("Tipo_Operacao"),
    col("TEstadia").cast("float")
).dropDuplicates()

atracacao_fato.write.mode("overwrite").parquet(os.path.join(data_lake_path, "atracacao_fato"))
atracacao_fato.write.jdbc(url=sql_server_url, table="atracacao_fato", mode="append", properties=sql_server_properties)
