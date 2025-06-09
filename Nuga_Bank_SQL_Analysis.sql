-- Create Relation's Costraints
ALTER TABLE transaction ADD PRIMARY KEY (transaction_id);
alter table fact_table add foreign key (transaction_id) references transaction(transaction_id);

ALTER TABLE customer ADD PRIMARY KEY (customer_id);
alter table fact_table add foreign key (customer_id) references customer(customer_id);

ALTER TABLE employee ADD PRIMARY KEY (employee_id);
alter table fact_table add foreign key (employee_id) references employee(employee_id);

-- select transactions' data with the highest amoount
select transaction_id,"Transaction_Date" ,"Transaction_Type" ,"Amount" from transaction 
where "Amount" = (select max("Amount") from transaction) ;

-- select type of transactions with highest  amount
select "Transaction_Type" , sum("Amount") from transaction group by "Transaction_Type";


-- select years with the highest amount of transactions

-- cast the Transaction_Date column from text to timestamp before apply query
SELECT date_part('year', "Transaction_Date"::timestamp) AS year, 
       SUM("Amount") 
FROM transaction
GROUP BY year;

-- find the running total amount of transactions for each customer along time
select "Customer_Name",
       "Customer_Address",
	   "Customer_City",
	   "Customer_State",
	   "Customer_Country",
	   "Email",
	   "Phone_Number",
	   "Amount",
	   sum("Amount") over(order by "Customer_Name") as total_transactions
from customer c
join fact_table f on c.customer_id = f.customer_id
join transaction t on f.transaction_id = t.transaction_id;


-- Rank customers based on their total amounts of transactions
select "Customer_Name",
	   sum("Amount") as total_transactions,
	   rank() over(order by sum("Amount") desc) as Rank_of_total_transactions
from customer c
join fact_table f on c.customer_id = f.customer_id
join transaction t on f.transaction_id = t.transaction_id
group by "Customer_Name";

-- Rank states based on their total amounts of transactions
select "Customer_State",
	   sum("Amount") as total_transactions,
	   rank() over(order by sum("Amount") desc) as Rank_of_total_transactions
from customer c
join fact_table f on c.customer_id = f.customer_id
join transaction t on f.transaction_id = t.transaction_id
group by "Customer_State";


-- Rank countriess based on their total amounts of transactions
select "Customer_Country",
	   sum("Amount") as total_transactions,
	   rank() over(order by sum("Amount") desc) as Rank_of_total_transactions
from customer c
join fact_table f on c.customer_id = f.customer_id
join transaction t on f.transaction_id = t.transaction_id
group by "Customer_Country";


--find companies based on their total amounts of transactions
select "Company", "Is_Active" , sum("Amount") as total_transactions from employee c
join fact_table f on c.employee_id = f.employee_id
join transaction t on f.transaction_id = t.transaction_id
group by "Company", "Is_Active" 
order by total_transactions desc;

-- Rank jobs on their total amounts of transactions
select "Customer_Name","Job_Title", sum("Amount") as total_transactions ,
rank() over(order by sum("Amount") desc)
from employee e
join fact_table f on e.employee_id = f.employee_id
join customer c on f.customer_id = c.customer_id
join transaction t on f.transaction_id = t.transaction_id
group by "Customer_Name","Job_Title" ;


-- Rank jobs on their total amounts of transactions for EACH job
select "Customer_Name","Job_Title", sum("Amount") as total_transactions ,
rank() over(partition by "Job_Title"  order by sum("Amount") desc)
from employee e
join fact_table f on e.employee_id = f.employee_id
join customer c on f.customer_id = c.customer_id
join transaction t on f.transaction_id = t.transaction_id
group by "Job_Title" , "Customer_Name";
;

-- find total transactions for each gender
select "Gender",sum("Amount") as total_transactions
from employee e
join fact_table f on e.employee_id = f.employee_id
join transaction t on f.transaction_id = t.transaction_id
group by "Gender"
order by total_transactions desc;


-- find the credintial information of customer according to the transactional amounts
select "Customer_Name",
       "Email",
	   "Phone_Number",
	   "Credit_Card_Number",
	   "IBAN",
	   "Currency_Code",
	   sum("Amount") as total_transactions
from customer  c
join fact_table f on f.customer_id = c.customer_id
join transaction t on f.transaction_id = t.transaction_id
group by "Customer_Name",
       "Email",
	   "Phone_Number",
	   "Credit_Card_Number",
	   "IBAN",
	   "Currency_Code"
order by total_transactions desc;


-- Rank currencies acoording to total_transactions
select "Currency_Code", sum("Amount") as total_transactions,
rank() over(order by sum("Amount") desc) as ranked_currencies
from fact_table f
join transaction t on f.transaction_id = t.transaction_id
group by "Currency_Code";

-- classify currencies based on total_transactions
select "Currency_Code",
       sum("Amount") as total_transactions,
       rank() over(order by sum("Amount") desc) as ranked_currencies ,
	   case
	        when sum("Amount") <= avg("Amount") then 'low currency'
			when sum("Amount") > avg("Amount") and sum("Amount") < max("Amount") then 'Medium Stron Currency'
			else 'Powerful currency'
		end as classified_currencies
from fact_table f
join transaction t on f.transaction_id = t.transaction_id
group by "Currency_Code";

-- Rank customers based on their total transactions for each categpry
select "Customer_Name" ,
       "Category" , 
	   sum("Amount") as total_transactions ,
       rank() over(partition by "Category" order by sum("Amount")  desc) as ranked_customers
from customer c
join fact_table f on f.customer_id = c.customer_id
join transaction t on f.transaction_id = t.transaction_id
group by "Customer_Name","Category";


-- arrange months according to number of transactions updated
select date_part('month', "Last_Updated"::timestamp) as months ,
       count("transaction_id") as number_of_transactions
from fact_table
group by months
order by number_of_Updated_transactions desc ;

-- find customer who made transactions lowe than the average onw

select "Customer_Name","Email","Phone_Number","IBAN","Amount" from customer c
join fact_table f 
on f.customer_id = c.customer_id
join
(with temporary_table (aveg) as(
      select avg("Amount") from transaction
) 
    select transaction_id , "Amount" from transaction, temporary_table
	where transaction.transaction_id < temporary_table.aveg) as a
on f.transaction_id = a.transaction_id	;