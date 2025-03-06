WITH DeduplicatedRewards AS (
  SELECT
    name AS validator_name,
    vote_account_pubkey AS vote_id,
    epoch,
    COALESCE(commission, 0) AS commission,
    COALESCE(activated_stake, 0) AS activated_stake,
    COALESCE(total_block_rewards_after_burn, 0) AS total_block_rewards_after_burn,
    COALESCE(mev_earned, 0) AS mev_earned
  FROM
    `kiln-devnet-0.solana_kiln_trillium_data.trillium_validator_rewards`
  WHERE
    epoch IS NOT NULL
)
SELECT
  validator_name,
  vote_id,
  epoch,
  activated_stake,
  total_block_rewards_after_burn,
  mev_earned,
  commission
FROM
  DeduplicatedRewards
ORDER BY
  validator_name ASC,
  epoch ASC
