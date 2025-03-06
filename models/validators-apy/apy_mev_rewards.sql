WITH MeVRewards AS (
  SELECT
    vr.validator_name,
    vr.epoch,
    TIMESTAMP(be.end_time) AS time,
    vr.mev_earned / vr.activated_stake AS mev_rewards_per_epoch,
    be.epochs_per_year
  FROM
    {{ ref('base_validator_rewards') }} vr
  JOIN
    {{ ref('base_epochs') }} be
  ON
    vr.epoch = be.epoch
  WHERE
    vr.activated_stake > 0
    AND vr.activated_stake IS NOT NULL
    AND vr.mev_earned > 0
    AND vr.mev_earned IS NOT NULL
    -- Limiter les valeurs extrêmes
    AND vr.mev_earned / vr.activated_stake < 1  -- Ajuste cette limite selon tes données
    AND be.epochs_per_year < 200  -- Ajuste selon la durée moyenne d'un epoch
)
SELECT
  validator_name,
  epoch,
  time,
  -- Limiter les calculs extrêmes avec un check ou un clip
  CASE
    WHEN mev_rewards_per_epoch > 0.1 THEN NULL  -- Optionnel : exclure les récompenses excessives
    ELSE POWER(1 + mev_rewards_per_epoch, epochs_per_year) - 1
  END AS mev_rewards_apy
FROM
  MeVRewards
WHERE
  mev_rewards_per_epoch IS NOT NULL
  AND epochs_per_year IS NOT NULL
  -- Filtrer les valeurs problématiques
  AND mev_rewards_per_epoch < 1  -- Empêche les dépassements numériques
ORDER BY
  validator_name ASC,
  epoch ASC
