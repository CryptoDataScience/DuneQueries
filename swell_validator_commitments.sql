SELECT evt_block_time AS block_time
, evt_block_number AS block_number
, SUM(value/1e18) AS eth_amount
, "from" AS tx_from
, evt_tx_hash AS tx_hash
FROM erc20_ethereum.evt_Transfer 
WHERE to = 0x325a0e5c84b4d961b19161956f57ae8ba5bb3c26 -- enzyme vault
AND contract_address = 0xae7ab96520de3a18e5e111b5eaab095312d7fe84
AND evt_block_number >= 18428812 -- block where vault was created
GROUP BY 1, 2, 4, 5