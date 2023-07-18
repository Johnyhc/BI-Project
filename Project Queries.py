import pandas as pd
from sqlalchemy import create_engine, text

tables = pd.read_json("./Codes/json_data/tables.json")
sales = pd.read_json("./Codes/json_data/sales.json")
items = pd.read_json("./Codes/json_data/items.json")

engine = create_engine('sqlite://', echo=False)
items.to_sql("items", con=engine)
tables.to_sql("tables", con=engine)
sales.to_sql("sales", con=engine)


def query(message, query):
    with engine.connect() as conn:
        res = conn.execute(text(query)).fetchall()

    print(message + '\n')
    print(pd.DataFrame(res), '\n')


print("PART 1".center(100, "_"))

q_1_1 = """
SELECT strftime('%H:00:00', start) as start_hour,
SUM(num_customers)/(SELECT COUNT(DISTINCT(date))/7 FROM tables) AS avg_customers
FROM tables
GROUP BY start_hour
ORDER BY start_hour;
"""

query("Average customers per hour", q_1_1)


q_1_2 = """
SELECT
strftime('%H:00:00', tables.start) as time,
CAST(SUM(price_per_table.total) AS REAL) / SUM(tables.num_customers) as ARPC

FROM tables JOIN(
  SELECT s.order_id, SUM(s.num_items * i.price) as total
  FROM items AS i JOIN(
    SELECT order_id, name, SUM(num_items) as num_items
    FROM sales
    GROUP BY order_id, name
    ) AS s ON s.name = i.name
  GROUP BY s.order_id) AS price_per_table ON tables.order_id = price_per_table.order_id

GROUP BY time
"""

query("ARPC per hour", q_1_2)


q_1_3 = """
SELECT
tables.num_customers,
AVG(price_per_table.total/tables.num_customers) AS ARPC

FROM tables JOIN(
  SELECT s.order_id, SUM(s.num_items * i.price) as total
  FROM items AS i JOIN(
    SELECT order_id, name, SUM(num_items) as num_items
    FROM sales
    GROUP BY order_id, name
    ) AS s ON s.name = i.name
  GROUP BY s.order_id) AS price_per_table ON tables.order_id = price_per_table.order_id

GROUP BY tables.num_customers
"""

query("ARPC per number of customers per table", q_1_3)

q_1_4 = """
SELECT day,
SUM(num_customers)/(SELECT COUNT(DISTINCT(date))/7 FROM tables) AS avg_customers
FROM tables
GROUP BY day
ORDER BY day;

"""
query("Average customers per day", q_1_4)

q_1_5 = """
SELECT
  AVG(price_per_table.total/tables.num_customers) AS ARPC,
  CASE
    WHEN tables.sitting_time < 30 THEN '<30'
    WHEN tables.sitting_time < 60 THEN '<60'
    WHEN tables.sitting_time < 80 THEN '<80'
    WHEN tables.sitting_time < 100 THEN '<100'
    WHEN tables.sitting_time < 120 THEN '<120'
    ELSE '>=120'
  END AS sitting_time_category

  FROM tables JOIN(
  SELECT s.order_id, SUM(s.num_items * i.price) as total
  FROM items AS i JOIN(
    SELECT order_id, name, SUM(num_items) as num_items
    FROM sales
    GROUP BY order_id, name
    ) AS s ON s.name = i.name
  GROUP BY s.order_id) AS price_per_table ON tables.order_id = price_per_table.order_id
  GROUP By sitting_time_category;
"""

query("ARPC per sitting time", q_1_5)

print("PART 2".center(100, "_"))

q_2_1 = """
SELECT
name,
COUNT(name) as count,
CAST(100 * COUNT(name) / SUM(COUNT(name)) OVER() AS FLOAT) AS sales_precent

FROM(
  SELECT
  order_id,
  name

  FROM sales
  WHERE order_id IN (
    SELECT
    tables.order_id

    FROM tables JOIN(
      SELECT s.order_id, SUM(s.num_items * i.price) as total
      FROM items AS i JOIN(
        SELECT order_id, name, SUM(num_items) as num_items
        FROM sales
        GROUP BY order_id, name
        ) AS s ON s.name = i.name
      GROUP BY s.order_id) AS price_per_table ON tables.order_id = price_per_table.order_id

    WHERE price_per_table.total/tables.num_customers > 200
    AND tables.num_customers > 1
    )

  GROUP BY order_id
)

GROUP BY name
ORDER BY count DESC
LIMIT 10
"""

query(" Find the top 10 items that appears in the tables with highest ARPC", q_2_1)


q_2_2 = """
SELECT
total_sales.name,
total_sales.sum_sales * items.price AS item_income

FROM (
  SELECT
  name,
  SUM(num_items) as sum_sales
  FROM sales
  WHERE name != 'Oth Chaser'
  GROUP BY name
) as total_sales JOIN items ON total_sales.name = items.name
ORDER BY item_income DESC
LIMIT 10
"""

query("Find the top 10 items with the highest income.", q_2_2)


q_2_3 = """
SELECT
items.subtype1 as category,
SUM(happy_hour.num_items * items.price) as category_income

FROM (
  SELECT
  name,
  num_items
  FROM sales
  WHERE name != 'Oth Chaser'
  AND time <= strftime('%H:00:00', "20:00:00")
  AND time >= strftime('%H:00:00', "18:00:00")
  AND day < 6
) as happy_hour JOIN items ON happy_hour.name = items.name

GROUP BY category
ORDER BY category_income DESC
LIMIT 5
"""

query("Find the top 5 categories with the highest income from Happy Hours sales.", q_2_3)

q_2_4 = """
SELECT
items.subtype2 as meal_type,
SUM(sales.num_items * items.price) as income

FROM (
  SELECT
  name,
  num_items
  FROM sales
  WHERE name != 'Oth Chaser') as sales JOIN items ON sales.name = items.name
  WHERE items.type = 'food'
GROUP BY meal_type
ORDER BY income DESC
LIMIT 5
"""

query("Find the top 5 subtype of meals with the highest income.", q_2_4)

q_2_5 = """
SELECT
items.name as beverage,
SUM(sales.num_items * items.price) as income

FROM (
  SELECT
  name,
  num_items
  FROM sales
  WHERE name != 'Oth Chaser') as sales JOIN items ON sales.name = items.name
  WHERE items.type = 'beverage'
GROUP BY beverage
ORDER BY income DESC
LIMIT 7
"""

query("Find the top 7 beverages with the highest income.", q_2_5)
