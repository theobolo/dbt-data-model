WITH raw_data AS (
    SELECT
        epoch,
        votesBase64
    FROM `kiln-devnet-0.solana_kiln_trillium_data.epochs_votes`
    WHERE epoch = 704 -- Filter for epoch 704
),
decoded_data AS (
    SELECT
        epoch,
        FROM_BASE64(votesBase64) AS binary_votes
    FROM raw_data
),
byte_split AS (
    SELECT
        epoch,
        ARRAY(
            SELECT AS STRUCT
                offset + 1 AS position,
                TO_HEX(SUBSTR(binary_votes, offset, 1)) AS byte_value
            FROM UNNEST(GENERATE_ARRAY(0, BYTE_LENGTH(binary_votes) - 1)) AS offset
        ) AS decoded_bytes
    FROM decoded_data
)
SELECT
    epoch,
    decoded_bytes
FROM byte_split
