USE data_bank;

-- --------------------------------------------------------- A. CUSTOMER NODE EXPLORATION -------------------------------------------------------------------------
-- 1. How many unique nodes are there in the data bank system? ---------------------------------------------
SELECT count(DISTINCT node_id) AS 'Unique_Nodes'
FROM customer_nodes;
-- 5

-- 2. What is the number of nodes per region? --------------------------------------------------------------
SELECT r.region_id, count(c.node_id) AS 'No. of Nodes' 
FROM customer_nodes c
JOIN regions r
	using (region_id)
GROUP BY r.region_id
ORDER BY r.region_id;

-- 3. How many customers are allocated to each region? -----------------------------------------------------
SELECT c.region_id, count(DISTINCT ct.customer_id) AS 'No. of Customers' 
FROM customer_nodes c
JOIN customer_transactions ct
	USING(customer_id)
GROUP BY region_id
ORDER BY region_id;

-- 4. How many days on average are customers reallocated to a different node? ------------------------------
SELECT AVG(DATEDIFF(end_date, start_date)) AS 'avg_number_of_day'
FROM customer_nodes
WHERE end_date != '9999-12-31';
-- Ans: 14.6 days



-- --------------------------------------------------------- B. CUSTOMER TRANSACTIONS -------------------------------------------------------------------------

-- 1. What is the unique count and total amount for each transaction type? ----------------------------------
SELECT 
	txn_type,  count(txn_type) 'Count', sum(txn_amount) 'Total Amount'
FROM customer_transactions
group by txn_type;

-- 2. What is the average total historical deposit counts and amounts for all customers? ----------------------
WITH deposit_customers AS 
	(
		SELECT 
			customer_id,  count(txn_type) 'Deposit_Count', sum(txn_amount) 'Total_Amount'
		FROM customer_transactions
		WHERE txn_type = 'deposit'
		GROUP BY customer_id
    )
SELECT 
	ROUND(AVG(Deposit_count),0) 'Avg Deposit Count',
    ROUND(AVG(Total_Amount),0) 'Avg Amount'
FROM deposit_customers;

-- 3. For each month — how many Data Bank customers make more than 1 deposit and either one purchase or withdrawal in a single month? -------
WITH transaction_summary AS (
	SELECT 
		customer_id,
		MONTH(txn_date) AS 'Month_Num',
		MONTHNAME(txn_date) AS 'Month',
		SUM(CASE WHEN txn_type = 'deposit' THEN 1 ELSE 0 END) AS 'Deposit',
		SUM(CASE WHEN txn_type = 'purchase' THEN 1 ELSE 0 END) AS 'Purchase',
		SUM(CASE WHEN txn_type = 'withdrawal' THEN 1 ELSE 0 END) AS 'Withdrawal'
	FROM customer_transactions
	GROUP BY customer_id, MONTH(txn_date), MONTHNAME(txn_date)
    ORDER BY Customer_id
    )
    
SELECT 
	Month,
    COUNT(DISTINCT customer_id) AS 'Filtered_Customers'
FROM transaction_summary
WHERE Deposit > 1 AND (Purchase = 1 OR Withdrawal = 1)
GROUP BY Month
ORDER BY Month;

-- 4. What is the closing balance for each customer at the end of the month? ---------------------------
WITH transaction_summary AS (
	SELECT 
		customer_id, 
		MONTH(txn_date) 'Month_Num',
		SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE -1 * txn_amount END) AS 'Amount_Calc'
	FROM customer_transactions
    GROUP BY customer_id, Month_Num
	ORDER BY customer_id, Month_Num
    )

SELECT 
	customer_id,
    Month_Num,
    SUM(Amount_Calc) OVER(PARTITION BY customer_id ORDER BY MONTH_Num ROWS BETWEEN
   			   UNBOUNDED PRECEDING AND CURRENT ROW) AS closing_balance
FROM transaction_summary
GROUP BY customer_id, Month_Num
ORDER BY customer_id;

-- 5. What is the percentage of customers who increase their closing balance by more than 5%? ------
WITH transaction_summary AS (
	SELECT 
		customer_id, 
		MONTH(txn_date) 'Month_Num',
		SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE -1 * txn_amount END) AS 'Amount_Calc'
	FROM customer_transactions
    GROUP BY customer_id, Month_Num
	ORDER BY customer_id
    ),
    
    Perc_Increase AS (
		SELECT
			customer_id,
            Month_Num,
            Amount_Calc,
            100 *(Amount_Calc - LAG(Amount_Calc) OVER(PARTITION BY customer_id ORDER BY	Month_Num))
   		 / NULLIF( LAG(Amount_Calc) OVER(PARTITION BY customer_id ORDER BY Month_Num), 2) AS 'Percent_Change'
		FROM transaction_summary
        )
SELECT 
	COUNT( DISTINCT customer_id) * 100 / (SELECT COUNT(DISTINCT customer_id) FROM customer_transactions) AS 'Total_Customers'
FROM Perc_Increase
WHERE Percent_Change > 5;



-- --------------------------------------------------------- C. DATA ALLOCATION CHALLENGE -------------------------------------------------------------------------

-- 1. running customer balance column that includes the impact each transaction -------------------------------------
WITH AmountCte AS (
   SELECT
   	customer_id,
   	txn_date,
   	CASE 
   		WHEN txn_type = 'deposit' THEN txn_amount 
   		ELSE -txn_amount END AS balance
   FROM customer_transactions
   ORDER BY customer_id
)
SELECT
   	customer_id,
   	txn_date,
   	SUM(balance) OVER(PARTITION BY customer_id ORDER BY txn_date 
   					  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
FROM AmountCte
GROUP BY customer_id, txn_date, balance
ORDER BY customer_id;

-- 2. customer balance at the end of each month ------------------------------------------------------------
WITH AmountCte AS(
   SELECT 
   	customer_id,
   	EXTRACT(MONTH from txn_date) AS month,
   	SUM(CASE
   		WHEN txn_type = 'deposit' THEN txn_amount
   		ELSE -txn_amount END) as amount
   FROM customer_transactions
   GROUP BY customer_id, month
   ORDER BY customer_id, month
)
SELECT 
   customer_id,
   month,
   SUM(amount) OVER(PARTITION  BY customer_id, month ORDER BY month 
   					 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance
FROM AmountCte
GROUP BY customer_id, month, amount

-- 3. minimum, average and maximum values of the running balance for each customer -------------------------------------------
WITH AmountCte AS (
   SELECT 
   	customer_id,
   	EXTRACT(MONTH from txn_date) AS month,
   	CASE
   		WHEN txn_type = 'deposit' THEN txn_amount
   		WHEN txn_type = 'purchase' THEN -txn_amount
   		WHEN txn_type = 'withdrawal' THEN -txn_amount END as amount
   FROM customer_transactions
   ORDER BY customer_id, month
),
RunningBalance AS (
   SELECT 
   	*,
   	SUM(amount) OVER (PARTITION BY customer_id, month ORDER BY customer_id, month
   		   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance
   FROM AmountCte
),
MonthlyAllocation AS(
   SELECT 
   	*,
   	LAG(running_balance, 1) OVER(PARTITION BY customer_id 
   								 ORDER BY customer_id, month) AS monthly_allocation
   FROM RunningBalance
)
SELECT
   month,
   SUM(monthly_allocation) AS total_allocation
FROM MonthlyAllocation
GROUP BY month
ORDER BY month;

SELECT
   month,
   SUM(
   	CASE WHEN monthly_allocation < 0 THEN 0 ELSE monthly_allocation END) AS total_allocation
FROM MonthlyAllocation
GROUP BY month
ORDER BY month;
