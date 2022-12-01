CREATE MATERIALIZED VIEW comparativa_escalafon_y_grado_por_organismo AS (
  (
    SELECT 
      nombre_organismo, 
      escalafon, 
      grado, 
      AVG(remuneracion_en_uyu) AS remuneraciones_promedio 
    FROM 
      public.miem 
    GROUP BY 
      nombre_organismo, 
      escalafon, 
      grado
  ) 
  UNION 
    (
      SELECT 
        nombre_organismo, 
        escalafon, 
        grado, 
        AVG(remuneracion_en_uyu) AS remuneraciones_promedio 
      FROM 
        public.mtop 
      GROUP BY 
        nombre_organismo, 
        escalafon, 
        grado
    ) 
  UNION 
    (
      SELECT 
        nombre_organismo, 
        escalafon, 
        grado, 
        AVG(remuneracion_en_uyu) AS remuneraciones_promedio
      FROM 
        public.ursec 
      GROUP BY 
        nombre_organismo, 
        escalafon, 
        grado
    ) 
  ORDER BY 
    escalafon, 
    grado, 
    nombre_organismo ASC
)
