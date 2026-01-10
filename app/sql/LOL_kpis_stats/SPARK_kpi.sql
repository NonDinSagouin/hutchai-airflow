SELECT 
    AVG(total_damage_dealt_to_champions / (game_duration / 60)) AS avg_damage_per_minute,
    AVG(physical_damage_dealt_to_champions / (game_duration / 60)) AS avg_physical_damage_per_minute,
    AVG(magic_damage_dealt_to_champions / (game_duration / 60)) AS avg_magic_damage_per_minute,
    AVG(true_damage_dealt_to_champions / (game_duration / 60)) AS avg_true_damage_per_minute,
    
    -- Répartition des dégâts infligés (%)
    AVG(physical_damage_dealt_to_champions * 100.0 / GREATEST(total_damage_dealt_to_champions, 1)) AS avg_physical_damage_dealt_pct,
    AVG(magic_damage_dealt_to_champions * 100.0 / GREATEST(total_damage_dealt_to_champions, 1)) AS avg_magic_damage_dealt_pct,
    AVG(true_damage_dealt_to_champions * 100.0 / GREATEST(total_damage_dealt_to_champions, 1)) AS avg_true_damage_dealt_pct,
    
    -- KDA par minute
    AVG(kills / (game_duration / 60)) AS avg_kills_per_minute,
    AVG(deaths / (game_duration / 60)) AS avg_deaths_per_minute,
    AVG(assists / (game_duration / 60)) AS avg_assists_per_minute,
    
    -- Multi-kill score (score pondéré des multi-kills)
    AVG(
        (double_kills * 2) + 
        (triple_kills * 4) + 
        (quadra_kills * 8) + 
        (penta_kills * 16)
    ) AS avg_multi_kill_score,
    
    -- Penta rate (pourcentage de parties avec penta)
    (SUM(CASE WHEN penta_kills > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS penta_rate,
    
    -- Dégâts subis par mort
    AVG(total_damage_taken / GREATEST(deaths, 1)) AS avg_damage_taken_per_death,

    AVG(physical_damage_taken / (game_duration / 60)) AS avg_physical_damage_taken_per_minute,
    AVG(magic_damage_taken / (game_duration / 60)) AS avg_magic_damage_taken_per_minute,
    AVG(true_damage_taken / (game_duration / 60)) AS avg_true_damage_taken_per_minute,
    
    -- Répartition des dégâts subis (%)
    AVG(physical_damage_taken * 100.0 / GREATEST(total_damage_taken, 1)) AS avg_physical_damage_taken_pct,
    AVG(magic_damage_taken * 100.0 / GREATEST(total_damage_taken, 1)) AS avg_magic_damage_taken_pct,
    AVG(true_damage_taken * 100.0 / GREATEST(total_damage_taken, 1)) AS avg_true_damage_taken_pct,
    
    -- Sustain (soins + mitigation par minute)
    AVG(total_heal / (game_duration / 60)) AS avg_heal_per_minute,
    
    -- Farm et économie
    AVG(total_minions_killed / (game_duration / 60)) AS avg_cs_per_minute,
    AVG(gold_earned / (game_duration / 60)) AS avg_gold_per_minute,
    AVG(gold_earned / GREATEST(total_minions_killed, 1)) AS avg_gold_per_cs,
    
    -- Statistiques additionnelles
    AVG((kills + assists) / GREATEST(deaths, 1)) AS avg_kda,
    COUNT(*) AS total_games,
    
    -- Statistiques par champion
    MAX(kills) AS best_kills,
    MAX(total_damage_dealt_to_champions) AS best_damage,
    MIN(deaths) AS best_deaths,
    
    -- Indices de performance
    AVG(
        (total_damage_taken / GREATEST(deaths, 1)) * 0.4 +
        (total_heal / (game_duration / 60)) * 0.3 +
        (1.0 / GREATEST(deaths / (game_duration / 60), 0.1)) * 0.3
    ) AS tanking_index,
    
    AVG(
        (total_damage_dealt_to_champions / (game_duration / 60)) * 0.5 +
        (kills / (game_duration / 60)) * 200 +
        (double_kills * 2 + triple_kills * 4 + quadra_kills * 8 + penta_kills * 16) * 5
    ) AS damage_index,
    
    AVG(
        (assists / (game_duration / 60)) * 300 +
        ((kills + assists) / GREATEST(deaths, 1)) * 100 +
        (total_heal / (game_duration / 60)) * 0.2
    ) AS support_index,
    
    -- Informations de groupement
    {{ select }}
FROM 
    stats
WHERE 
    game_duration > 120 -- Filtrer les parties de plus de 2 minutes
GROUP BY 
    {{ groupby }}
ORDER BY 
    game_name, total_games DESC