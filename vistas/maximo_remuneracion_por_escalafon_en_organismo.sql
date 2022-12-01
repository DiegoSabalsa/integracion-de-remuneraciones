CREATE MATERIALIZED VIEW maximo_remuneracion_por_escalafon_en_organismo AS (SELECT 
  cepo.nombre_organismo, 
  maximos_agrupados.escalafon, 
  maximos_agrupados.maximo_remuneracion 
FROM 
  (
    SELECT 
      escalafon, 
      max(remuneraciones_promedio) as maximo_remuneracion 
    FROM 
      comparativa_escalafones_por_organismo 
    GROUP BY 
      escalafon 
    HAVING 
      escalafon <> 'S/D'
  ) as maximos_agrupados 
  JOIN comparativa_escalafones_por_organismo as cepo ON cepo.escalafon = maximos_agrupados.escalafon 
  AND cepo.remuneraciones_promedio = maximos_agrupados.maximo_remuneracion)