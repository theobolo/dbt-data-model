-- models/kiln1_analysis.sql

WITH apy_data AS (
  SELECT
    DISTINCT -- Suppression des doublons
    v.apy AS net_apy,
    v.voteId AS validator_id,
    v.epoch AS epoch
  FROM 
    `kiln-devnet-0.solana_kiln_trillium_data.svtone_validators_history` v
),

validator_infos AS (
  SELECT
    vr.validator_name,
    vr.vote_id AS validator_id,
    vr.commission,
    vr.epoch
  FROM 
    {{ ref('base_validator_rewards') }} vr -- Référence au modèle DBT de base pour les récompenses des validateurs
  WHERE
    vr.validator_name IS NOT NULL -- Filtrer les enregistrements avec des noms de validateurs valides
),

epochs_data AS (
  SELECT
    DISTINCT -- Suppression des doublons
    epoch,
    TIMESTAMP(end_time) AS time -- Conversion du champ temporel pour une agrégation cohérente
  FROM 
    {{ ref('base_epochs') }} -- Référence au modèle DBT de base pour les données d'épochs
  WHERE
    end_time IS NOT NULL -- Filtrer les enregistrements avec des end_time valides
),

cluster_avg_apy AS (
  SELECT
    epoch,
    AVG(apy) AS cluster_avg_inflation_apy -- Calcul de la moyenne des APY pour le cluster par epoch
  FROM
    `kiln-devnet-0.solana_kiln_trillium_data.svtone_apy_avg`
  WHERE
    apy IS NOT NULL -- Filtrer les enregistrements avec des APY valides
  GROUP BY
    epoch
),

calculated_data AS (
  SELECT
    vi.validator_name,
    ed.time,
    ed.epoch,
    CASE 
      WHEN vi.commission = 100 THEN NULL
      ELSE (ad.net_apy / (1 - vi.commission / 100)) * 100 -- Calcul de l'Inflation APY ajustée pour la commission
    END AS inflation_apy,
    ca.cluster_avg_inflation_apy
  FROM
    apy_data ad
  INNER JOIN 
    validator_infos vi
      ON ad.validator_id = vi.validator_id
  INNER JOIN 
    epochs_data ed
      ON ad.epoch = ed.epoch
  LEFT JOIN
    cluster_avg_apy ca
      ON ad.epoch = ca.epoch
  WHERE
    ad.net_apy IS NOT NULL -- Filtrer les enregistrements avec des APY valides
),

final_selection AS (
  SELECT
    validator_name,
    time,
    epoch,
    inflation_apy,
    (cluster_avg_inflation_apy * 100) as cluster_avg_inflation_apy,
    SAFE_DIVIDE((inflation_apy - cluster_avg_inflation_apy), cluster_avg_inflation_apy) * 100 AS percent_difference_vs_cluster
  FROM
    calculated_data
  WHERE
    time IS NOT NULL -- Exclure les timestamps non valides
    AND inflation_apy >= 0.1 -- Filtrer les APY proches de 0%
)

SELECT
  *
FROM
  final_selection
ORDER BY 
  validator_name ASC,
  time ASC
