SELECT *
FROM (
    SELECT date, category, quantity
    FROM retail_transactions
)
PIVOT(
    SUM(quantity)
    FOR category IN ('Apparel', 'Electronics', 'Books')
)
ORDER BY date;



SELECT
  TRANSACTION_ID,
  CUSTOMER_ID,
  DATE,
  TIME,
  PRODUCT_NAME,
  CATEGORY,
  METRIC_TYPE,
  METRIC_VALUE
FROM (
  SELECT
    TRANSACTION_ID,
    CUSTOMER_ID,
    DATE,
    TIME,
    PRODUCT_NAME,
    CATEGORY,
    CAST(QUANTITY AS VARCHAR) AS QUANTITY,
    CAST(PRICE AS VARCHAR) AS PRICE
  FROM retail_transactions
)
UNPIVOT (
  METRIC_VALUE FOR METRIC_TYPE IN (QUANTITY, PRICE)
) AS UNPVT;



CREATE OR REPLACE TABLE raw_source (
  src VARIANT
);



INSERT INTO raw_source (src)
SELECT PARSE_JSON('
{
  "device_type": "server",
  "version": 2.6,
  "events": [
    {
      "f": 83,
      "rv": "15219.64,783.63,48674.48,84679.52,27499.78,2178.83,0.42,74900.19",
      "t": 1437560931139,
      "v": {
        "ACHZ": 42869,
        "ACV": 709489,
        "DCA": 232,
        "DCV": 62287,
        "ENJR": 2599,
        "ERRS": 205,
        "MXEC": 487,
        "TMPI": 9
      },
      "vd": 54,
      "z": 1437644222811
    },
    {
      "f": 1000083,
      "rv": "8070.52,54470.71,85331.27,9.10,70825.85,65191.82,46564.53,29422.22",
      "t": 1437036965027,
      "v": {
        "ACHZ": 6953,
        "ACV": 346795,
        "DCA": 250,
        "DCV": 46066,
        "ENJR": 9033,
        "ERRS": 615,
        "MXEC": 0,
        "TMPI": 112
      },
      "vd": 626,
      "z": 1437660796958
    }
  ]
}');




SELECT
  src:device_type::STRING AS device_type,
  src:version::FLOAT AS version,
  f.value:f::NUMBER AS f_value,
  f.value:rv::STRING AS rv,
  f.value:t::NUMBER AS t,
  f.value:v.ACHZ::NUMBER AS achz,
  f.value:v.ACV::NUMBER AS acv,
  f.value:v.DCA::NUMBER AS dca,
  f.value:v.DCV::NUMBER AS dcv,
  f.value:v.ENJR::NUMBER AS enjr,
  f.value:v.ERRS::NUMBER AS errs,
  f.value:v.MXEC::NUMBER AS mxec,
  f.value:v.TMPI::NUMBER AS tmpi,
  f.value:vd::NUMBER AS vd,
  f.value:z::NUMBER AS z
FROM raw_source,
LATERAL FLATTEN(input => src:events) f;
