CREATE DATABASE db_ej1;

CREATE EXTERNAL TABLE db_ej1.h_aerop_tab ( 
aeropuerto string,
oac string,
iata string,
tipo string,
denominacion string,
coordenadas string,
latitud string,
longitud string,
elev float,
uom_elev string,
ref string,
distancia_ref float,
direccion_ref string,
condicion string,
control string,
region string,
uso string,
trafico string,
sna string,
concesionado string,
provincia string
)
row format delimited
FIELDS TERMINATED BY ','
;

CREATE EXTERNAL TABLE db_ej1.h_info_tab (
fecha date,
horaUTC string,
clase_de_vuelo string,
clasificacion_de_vuelo string,
tipo_de_movimiento string,
aeropuerto string,
origen_destino string,
aerolinea_nombre string,
aeronave string,
pasajeros int
)
row format delimited
FIELDS TERMINATED BY ";"
;