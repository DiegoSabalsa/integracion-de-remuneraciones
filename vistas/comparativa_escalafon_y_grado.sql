
CREATE MATERIALIZED VIEW comparativa_escalafon_y_grado AS (
  SELECT 
    escalafon, 
    grado, 
    AVG(remuneraciones_promedio) as remuneraciones_promedio 
  FROM 
    (
      (
        SELECT 
          escalafon, 
          grado, 
          AVG(remuneracion_en_uyu) as remuneraciones_promedio 
        FROM 
          public.miem 
        GROUP BY 
          escalafon, 
          grado
      ) 
      UNION 
        (
          SELECT 
            escalafon, 
            grado, 
            AVG(remuneracion_en_uyu) as remuneraciones_promedio 
          FROM 
            public.mtop 
          GROUP BY 
            escalafon, 
            grado
        ) 
      UNION 
        (
          SELECT 
            escalafon, 
            grado, 
            AVG(remuneracion_en_uyu) as remuneraciones_promedio 
          FROM 
            public.ursec 
          GROUP BY 
            escalafon, 
            grado
        ) 
      ORDER BY 
        escalafon, 
        grado ASC
    ) AS escalafones_y_remuneraciones 
  GROUP BY 
    escalafon, 
    grado 
  HAVING 
    escalafon <> 'S/D'
)
