WITH DeduplicatedEpochs AS (
  SELECT
    epoch,
    TIMESTAMP(COALESCE(min_block_time_calendar, '1970-01-01 00:00:00')) AS start_time, -- Default to Unix epoch if NULL
    TIMESTAMP(COALESCE(max_block_time_calendar, '1970-01-01 00:00:00')) AS end_time, -- Default to Unix epoch if NULL
    epochs_per_year,
    ROW_NUMBER() OVER (PARTITION BY epoch ORDER BY max_block_time_calendar DESC) AS row_num -- Keep the latest max_block_time_calendar
  FROM
    `kiln-devnet-0.solana_kiln_trillium_data.trillium_epochs_data`
  WHERE
    epochs_per_year IS NOT NULL -- Exclude rows with NULL epochs_per_year
)
SELECT
  epoch,
  start_time,
  end_time,
  epochs_per_year
FROM
  DeduplicatedEpochs
WHERE
  row_num = 1 -- Keep only the first row per epoch
ORDER BY
  epoch ASC
