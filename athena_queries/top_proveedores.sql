SELECT
    country_of_origin as pais_origen,
    count(1) as cantidad_proveedores
FROM
    dim_proveedores
GROUP BY
    country_of_origin