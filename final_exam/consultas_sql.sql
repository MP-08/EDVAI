
#PUNTO 6
SELECT COUNT(*) AS total_registros
FROM db_ej1.h_info_tab
WHERE fecha BETWEEN '2021-12-01' AND '2021-12-31';

#PUNTO 7
SELECT SUM(pasajeros) FROM db_ej1.h_info_tab
WHERE lower(aerolinea_nombre) like '%aerolineas argentinas%'
AND fecha BETWEEN '2021-01-01' AND '2022-06-30';

#PUNTO 8

WITH salidas AS (
    SELECT 
        fecha,
        horautc,
        a.aeropuerto AS aeropuerto_salida,
        ads.denominacion AS ciudad_salida,  -- Cambio aquí
        a.origen_destino AS codigo_aeropuerto_arribo,
        ada.denominacion AS ciudad_arribo,  -- Cambio aquí
        COALESCE(pasajeros, 0) AS pasajeros
    FROM ejercicio1db.h_info_tabla a
    INNER JOIN ejercicio1db.h_tabla_aerop ads ON a.aeropuerto = ads.aeropuerto
    INNER JOIN ejercicio1db.h_tabla_aerop ada ON a.origen_destino = ada.aeropuerto
    WHERE tipo_de_movimiento = 'Despegue'
),
arribos AS (
    SELECT 
        fecha,
        horautc,
        a.origen_destino AS aeropuerto_salida,
        ads.denominacion AS ciudad_salida,  -- Cambio aquí
        a.aeropuerto AS codigo_aeropuerto_arribo,
        ada.denominacion AS ciudad_arribo,  -- Cambio aquí
        COALESCE(pasajeros, 0) AS pasajeros
    FROM ejercicio1db.h_info_tabla a
    INNER JOIN ejercicio1db.h_tabla_aerop ads ON a.origen_destino = ads.aeropuerto
    INNER JOIN ejercicio1db.h_tabla_aerop ada ON a.aeropuerto = ada.aeropuerto
    WHERE tipo_de_movimiento = 'Aterrizaje'
)
SELECT * 
FROM salidas
UNION ALL
SELECT * 
FROM arribos
WHERE fecha BETWEEN '2021-01-01' AND '2022-06-30'
ORDER BY fecha DESC;

#PUNTO 9 
select aerolinea_nombre,
sum(pasajeros) total_pasajeros
from db_ej1.h_info_tab
where aerolinea_nombre is not NULL and aerolinea_nombre <> '0'
and fecha between '2021-01-01' and '2022-06-30'
group by aerolinea_nombre 
order by total_pasajeros desc limit 10;

#PUNTO 10 

select a.aeronave,
count (a.aeronave) cantidad_de_vuelos
from db_ej1.h_info_tab a
inner join db_ej1.h_aerop_tab d
on a.aeropuerto=d.aeropuerto 
where a.fecha between '2021-01-01' and '2022-06-30'
and a.aeronave is not NULL 
and a.aeronave <> '0'
and a.tipo_de_movimiento='Despegue'
and lower(d.provincia) like '%buenos aires%'
group by a.aeronave 
order by cantidad_de_vuelos DESC
limit 10;
