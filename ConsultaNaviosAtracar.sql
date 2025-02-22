SELECT 
    CASE 
        WHEN SGUF = 'CE' THEN 'Ceará'
        WHEN Regiao_Geografica = 'Nordeste' THEN 'Nordeste'
        ELSE 'Brasil' 
    END AS Localidade,
    COUNT(IDAtracacao) AS Numero_Atracacoes,
    COALESCE(
        (COUNT(IDAtracacao) - LAG(COUNT(IDAtracacao)) OVER (PARTITION BY SGUF ORDER BY Ano, Mes)) * 100.0 / NULLIF(LAG(COUNT(IDAtracacao)) OVER (PARTITION BY SGUF ORDER BY Ano, Mes), 0),
        0
    ) AS Variacao_Atracacoes,
    AVG(TEsperaAtracacao) AS Tempo_Espera_Medio,
    AVG(TAtracado) AS Tempo_Atracado_Medio,
    Mes,
    Ano
FROM atracacao_fato
WHERE SGUF IN ('CE', 'Nordeste', 'Brasil')
GROUP BY 
    CASE 
        WHEN SGUF = 'CE' THEN 'Ceará'
        WHEN Regiao_Geografica = 'Nordeste' THEN 'Nordeste'
        ELSE 'Brasil' 
    END, 
    Ano, 
    Mes, 
    SGUF
ORDER BY Ano DESC, Mes DESC, Localidade;
