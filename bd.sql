CREATE TABLE estaciones (
    id_estacion   TEXT PRIMARY KEY,      
    ubicacion     TEXT NOT NULL,          
    estado        TEXT NOT NULL DEFAULT 'DESCONECTADA',
    fecha_alta    TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE operarios (
    id_operario   TEXT PRIMARY KEY,     
    nombre        TEXT
);

CREATE TABLE riegos (
    id_riego          INTEGER PRIMARY KEY AUTOINCREMENT,
    id_estacion       TEXT NOT NULL,
    id_operario       TEXT,              
    inicio            TEXT DEFAULT CURRENT_TIMESTAMP,
    fin               TEXT,
    volumen_total_l   REAL,
    duracion_s        INTEGER,
    motivo_fin        TEXT,               
    FOREIGN KEY (id_estacion) REFERENCES estaciones(id_estacion),
    FOREIGN KEY (id_operario) REFERENCES operarios(id_operario)
);