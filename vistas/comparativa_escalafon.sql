
CREATE MATERIALIZED VIEW comparativa_escalafon AS (
  SELECT 
    escalafon, 
    AVG(remuneraciones_promedio) as remuneraciones_promedio  
  FROM 
    (
      (
        SELECT 
          escalafon, 
          AVG(remuneracion_en_uyu) as remuneraciones_promedio 
        FROM 
          public.miem 
        GROUP BY 
          escalafon
      ) 
      UNION 
        (
          SELECT 
            escalafon, 
            AVG(remuneracion_en_uyu) as remuneraciones_promedio 
          FROM 
            public.mtop 
          GROUP BY 
            escalafon
        ) 
      UNION 
        (
          SELECT 
            escalafon, 
            AVG(remuneracion_en_uyu) as remuneraciones_promedio 
          FROM 
            public.ursec 
          GROUP BY 
            escalafon
        ) 
      ORDER BY 
        escalafon ASC
    ) AS escalafones_y_remuneraciones 
  GROUP BY 
    escalafon 
  HAVING 
    escalafon <> 'S/D'
)
