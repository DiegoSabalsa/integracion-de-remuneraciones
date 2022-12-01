
CREATE MATERIALIZED VIEW comparativa_escalafones_por_organismo AS (
  (
    SELECT 
      nombre_organismo, 
      escalafon, 
      AVG(remuneracion_en_uyu) AS remuneraciones_promedio 
    FROM 
      public.miem 
    GROUP BY 
      nombre_organismo, 
      escalafon
  ) 
  UNION 
    (
      SELECT 
        nombre_organismo, 
        escalafon, 
        AVG(remuneracion_en_uyu) AS remuneraciones_promedio 
      FROM 
        public.mtop 
      GROUP BY 
        nombre_organismo, 
        escalafon
    ) 
  UNION 
    (
      SELECT 
        nombre_organismo, 
        escalafon, 
        AVG(remuneracion_en_uyu) AS remuneraciones_promedio
      FROM 
        public.ursec 
      GROUP BY 
        nombre_organismo, 
        escalafon
    ) 
  ORDER BY 
    escalafon, 
    nombre_organismo ASC
)

