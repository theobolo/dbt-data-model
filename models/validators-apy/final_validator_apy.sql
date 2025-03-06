WITH CombinedAPYs AS (
  SELECT
    COALESCE(ia.validator_name, bra.validator_name, mra.validator_name) AS validator_name,
    COALESCE(ia.epoch, bra.epoch, mra.epoch) AS epoch,
    -- L'APY d'inflation est déjà exprimé en pourcentage dans apy_inflation
    COALESCE(ia.inflation_apy, 0) AS inflation_apy,
    -- Les APY des block rewards et MEV sont calculés en décimal et doivent être convertis en %
    COALESCE(bra.block_rewards_apy, 0) AS block_rewards_apy,
    COALESCE(mra.mev_rewards_apy, 0) AS mev_rewards_apy
  FROM
    {{ ref('apy_inflation') }} ia
  FULL OUTER JOIN
    {{ ref('apy_block_rewards') }} bra
      ON ia.validator_name = bra.validator_name
      AND ia.epoch = bra.epoch
  FULL OUTER JOIN
    {{ ref('apy_mev_rewards') }} mra
      ON COALESCE(ia.validator_name, bra.validator_name) = mra.validator_name
      AND COALESCE(ia.epoch, bra.epoch) = mra.epoch
),
EpochTime AS (
  -- Récupérer pour chaque epoch le max_block_time (end_time) de base_epochs
  SELECT
    epoch,
    TIMESTAMP(end_time) AS max_block_time
  FROM
    {{ ref('base_epochs') }}
)
SELECT
  ca.validator_name,
  ca.epoch,
  et.max_block_time AS time,
  ca.inflation_apy,
  ca.block_rewards_apy * 100 AS block_rewards_apy,
  ca.mev_rewards_apy * 100 AS mev_rewards_apy,
  -- Le total APY est la somme de l'APY d'inflation (déjà en %) et des APY des block et MEV convertis en %
  ca.inflation_apy 
    + (ca.block_rewards_apy * 100) 
    + (ca.mev_rewards_apy * 100) AS total_apy
FROM
  CombinedAPYs ca
JOIN
  EpochTime et
  ON ca.epoch = et.epoch
ORDER BY
  ca.validator_name ASC,
  ca.epoch ASC
