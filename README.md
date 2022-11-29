# Credit-Card-Fraud-Detection-Data-Engineering


Explanation of the solution to the batch layer problem

Task 1: Load the transactions history data (card_transactions.csv) in a NoSQL database.
Load the transaction history data to hbase and also to hive using the relavent commands mentioned in documents.

Task 2: Ingest the relevant data from AWS RDS to Hadoop.
Ingest data to hdfs from rds path/details given and load this as well to hive. Specifically member_score data

Task 3: Create a look-up table with columns specified earlier in the problem statement.
First, create lookup table from hive which maps to hbase and next step is to run a query which basically joins the card_transactions and member_score table to get the latest entry for each member.

Later, join the above query with UCL computed select statement which computed moving average and standard deviation and applies formula (avg + 3 x Std.Deveation) to have all fields to fill data in loop_up_table.

Finally add these columns to newely created look up table which basically reflects in hbase too


SCRIPT:
insert into transactions.look_up_table_hbase ( card_id, ucl, postcode, transaction_dt, score)
select t1.card_id, us.ucl, t1.postcode, t1.transaction_dt, t1.score
from 
(SELECT t.card_id
    ,t.postcode
    ,t.transaction_dt
    ,t.score
FROM (
    SELECT s.member_id, c.card_id
        ,c.postcode
        ,c.transaction_dt
        ,s.score
        ,ROW_NUMBER() OVER (
            PARTITION BY card_id ORDER BY unix_timestamp(transaction_dt,'dd-MM-yyyy hh:mm:ss') DESC
            ) AS ROW_NUMBER   
    FROM  transactions.card_transactions c
    join transactions.member_score s on c.member_id=s.member_id
    order by c.card_id
    ) t 
WHERE t.ROW_NUMBER <= 1) t1
join (SELECT t.card_id, cast (avg(t.amount)+3*STDDEV(t.amount) as  int) as ucl
FROM (
    SELECT s.member_id, c.card_id
        ,c.postcode
        ,c.amount
        ,c.transaction_dt
        ,s.score
        ,ROW_NUMBER() OVER (
            PARTITION BY card_id ORDER BY unix_timestamp(transaction_dt,'dd-MM-yyyy hh:mm:ss') DESC
            ) AS ROW_NUMBER   
    FROM  transactions.card_transactions c
    join transactions.member_score s on c.member_id=s.member_id
    order by c.card_id
    ) t 
WHERE t.ROW_NUMBER <= 10
group by t.card_id) us on us.card_id=t1.card_id;

Task 4: After creating the table, you need to load the relevant data in the lookup table.

hive> select * from transactions.look_up_table limit 10;
OK
340028465709212 319613580       24658   02-01-2018 03:25:35     233
340054675199675 14156079        50140   15-01-2018 19:43:23     631
340082915339645 15285685        17844   26-01-2018 19:03:47     407
340134186926007 909246042       67576   18-01-2018 23:12:50     614
340265728490548 16084916        72435   21-01-2018 02:07:35     202
340268219434811 12507323        62513   16-01-2018 04:30:05     415
340379737226464 14198310        26656   27-01-2018 00:19:47     229
340383645652108 14091750        34734   29-01-2018 01:29:12     645
340803866934451 10843341        87525   31-01-2018 04:23:57     502
340889618969736 13217942        61341   31-01-2018 21:57:18     330
Time taken: 0.088 seconds, Fetched: 10 row(s)


Task 5: Create a streaming data processing framework that ingests real-time POS transaction data from Kafka. The transaction data is then validated based on the three rules’ parameters (stored in the NoSQL database) 

The steps followed to do this task includes following steps
•	Import all necessary libraries and functions.
•	Define spark context and add .py files with csv.
•	Connect to kafka topic using
Bootstrap-server: 18.211.252.152
Port Number: 9092 
Topic: transactions-topic-verified
•	Read kafka stream into required schema to map data.
•	Look Up Table Name: look_up_table
Card Transaction table Name: card_transactions
•	Defining following UDF to perform required activities and determine whether transaction is fraudulent or genuine.
FUNCTION	INPUT	OUTPUT
ucl_data	CARD_ID	UCL (look_up_table)
score_data	CARD_ID	Credit Score (look up tab)
ostcode_data	CARD_ID	post code (look up table)
distance_calc	post codes (lookup table & kafka stream)	Distance between 2 locations of current
transaction and previous transaction
time_cal	transaction date (lookup table & kafka
stream)	difference between transaction dates in
seconds.
TransD_data	CARD_ID	transaction date (look up table)
speed_calc	Distance & Time calculated from above
distance_calc & time_cal functions	Distance & Time calculated from
distance_calc & time_cal functions
status_res	Amount from current transaction read thru kafka stream, UCL from look up table, Credit_Score (look up table) &
Speed calculated (udf)	Status of transaction (genuine or fraud)

•	Executing UDF sequencially. Hence, deriving if transaction is fraud or genuine. These functions work as agents to derive inputs to function status_res (function H).
•	The rules performed on inputs supplied to function H.
If current transaction amount is greater than UCL of look up table for that card_id, mark transaction as Fraud. Else, proceed to check below:
- If credit score of that card_id under process is less than 250, reject transaction as FRAUD. Else, proceed.
- If speed calculated is greater than 250, recognize the transaction as “FRAUD”. If speed is between 0 and 250, mark the transaction as genuine.
•	To summarize, a transaction is qualified to be genuine only when:
- Credit score of member is greater than 200,
- Speed is between 0 & 250
- Amount on current transaction is less than UCL calculated.
•	Functions “A”, “B”, “C”, “F” & “H” contact dao.py to call the look up table (given above) for designated purposes.
- In process of calling dao.py from this driver.py file, I fo called “Import” which loads other .py files in same directory.
- Establishing spark context to add python files and csv files before command import.
•	Function “D” uses geomap.py to calculate distance between last transaction & current transaction locations that is used in calculating speed which is one of factors for determining status of transaction.
•	Function “H” status_res also calls look_up_table using write_data function when transaction is genuine.
- It also updates card_transactions table with latest information of posid, amount, transaction date and member ID.


Command to run:
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 --py-files src.zip --files uszipsv.csv driver.py

 



 

