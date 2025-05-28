
SELECT
  Policy_Holder_ID,
  Date_of_Claim,
  Claim_Amount,
  SUM(Claim_Amount) OVER (
    PARTITION BY Policy_Holder_ID
    ORDER BY Date_of_Claim
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS Running_Total_Claims
FROM auto_claims
ORDER BY Policy_Holder_ID, Date_of_Claim;



SELECT
  Vehicle_Make,
  Claim_ID,
  Claim_Amount,
  RANK() OVER (
    PARTITION BY Vehicle_Make
    ORDER BY Claim_Amount DESC
  ) AS Claim_Rank
FROM auto_claims;



SELECT
  Company,
  Year,
  NetIncome,
  ROW_NUMBER() OVER (
    PARTITION BY Company
    ORDER BY Year
  ) AS Yearly_Row_Number
FROM financials;
