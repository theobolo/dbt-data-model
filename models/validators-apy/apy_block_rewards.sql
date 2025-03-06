WITH block_rewards AS (
  SELECT
    vr.validator_name,
    vr.epoch,
    TIMESTAMP(ed.end_time) AS time,
    vr.total_block_rewards_after_burn / vr.activated_stake AS block_rewards_per_epoch,
    ed.epochs_per_year
  FROM
    {{ ref('base_validator_rewards') }} vr
  JOIN
    {{ ref('base_epochs') }} ed
  ON
    vr.epoch = ed.epoch
  WHERE
    vr.activated_stake > 0
    AND vr.activated_stake IS NOT NULL
    AND vr.total_block_rewards_after_burn IS NOT NULL
    AND ed.epochs_per_year IS NOT NULL
    -- Limite pour éviter les valeurs extrêmes
    AND vr.total_block_rewards_after_burn / vr.activated_stake < 1
    AND ed.epochs_per_year < 200
)
SELECT
  validator_name,
  epoch,
  time,
  -- Limiter les calculs extrêmes avec une logique conditionnelle
  CASE
    WHEN block_rewards_per_epoch > 0.1 THEN NULL  -- Optionnel : ignorer les valeurs trop élevées
    ELSE POWER(1 + block_rewards_per_epoch, epochs_per_year) - 1
  END AS block_rewards_apy
FROM
  block_rewards
WHERE
  block_rewards_per_epoch IS NOT NULL
  AND epochs_per_year IS NOT NULL
  AND block_rewards_per_epoch < 1  -- Empêche les dépassements numériques
ORDER BY
  validator_name ASC,
  epoch ASC
