WITH unadjusted_flows AS (
    SELECT block_time
    , block_number
    , depositor_address
    , tx_from
    , withdrawal_address
    , amount_staked
    , amount_full_withdrawn
    , amount_partial_withdrawn
    , entity
    , entity_category
    , sub_entity
    , sub_entity_category
    , validator_index
    , tx_hash
    , deposit_index
    FROM (
        SELECT block_time
        , block_number
        , depositor_address
        , tx_from
        , withdrawal_address
        , amount_staked
        , amount_full_withdrawn
        , amount_partial_withdrawn
        , entity
        , entity_category
        , sub_entity
        , sub_entity_category
        , validator_index
        , pubkey
        , tx_hash
        , deposit_index
        FROM staking_ethereum.flows
        
        UNION ALL
    
        SELECT block_time
        , block_number
        , NULL AS depositor_address
        , tx_from
        , NULL AS withdrawal_address
        , eth_amount AS amount_staked
        , 0 AS amount_full_withdrawn
        , 0 AS amount_partial_withdrawn
        , 'Diva (Pre-launch)' AS entity
        , 'Liquid Staking' AS entity_category
        , NULL AS sub_entity
        , NULL AS sub_entity_category
        , -1 AS validator_index
        , NULL AS pubkey
        , tx_hash
        , NULL AS deposit_index
        FROM query_3090893
        
        UNION ALL
    
        SELECT block_time
        , block_number
        , NULL AS depositor_address
        , tx_from
        , NULL AS withdrawal_address
        , eth_amount AS amount_staked
        , 0 AS amount_full_withdrawn
        , 0 AS amount_partial_withdrawn
        , 'Swell (Pre-launch)' AS entity
        , 'Liquid Staking' AS entity_category
        , NULL AS sub_entity
        , NULL AS sub_entity_category
        , -1 AS validator_index
        , NULL AS pubkey
        , tx_hash
        , NULL AS deposit_index
        FROM query_3266360
        )
    )

, other_independent_stakers AS (
    SELECT entity
    FROM unadjusted_flows
    WHERE entity_category = 'Solo Staker'
    GROUP BY 1
    HAVING SUM(amount_staked)-COALESCE(SUM(amount_full_withdrawn), 0) < 4500
    )

, other_independent_stakers_sub AS (
    SELECT sub_entity
    FROM unadjusted_flows
    WHERE sub_entity IS NOT NULL 
    AND entity_category = 'Solo Staker'
    GROUP BY 1
    HAVING SUM(amount_staked)-COALESCE(SUM(amount_full_withdrawn), 0) < 4500
    )

SELECT block_time
, block_number
, depositor_address
, tx_from
, withdrawal_address
, amount_staked
, amount_full_withdrawn
, amount_partial_withdrawn
, CASE WHEN entity_category='Solo Staker' AND entity IN (SELECT entity FROM other_independent_stakers) THEN 'Other Solo Stakers'
    ELSE entity
    END AS entity
, CASE WHEN entity_category IN ('CEX', 'Solo Staker', 'Staking Pool') THEN entity_category || 's' ELSE entity_category END AS entity_category
, CASE WHEN sub_entity_category='Solo Staker' AND sub_entity IN (SELECT sub_entity FROM other_independent_stakers_sub) THEN 'Other Solo Stakers'
    ELSE sub_entity
    END AS sub_entity
, CASE WHEN sub_entity_category IN ('CEX', 'Solo Staker', 'Staking Pool') THEN sub_entity_category || 's' ELSE sub_entity_category END AS sub_entity_category
, validator_index
, tx_hash
, deposit_index
FROM unadjusted_flows