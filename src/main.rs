use dotenv::dotenv;
use sqlx::{PgPool};
use std::{env};
use anyhow::Result;
use elasticsearch::{Elasticsearch, BulkParts};
use serde_json::{json, Value};
use serde::{Serialize};
use elasticsearch::http::request::JsonBody;
use std::time::Instant;
use rayon::prelude::*;
use std::collections::HashMap;

#[derive(Debug, Serialize, Clone)]
pub struct Customer<> {
    pub customer_id: i64,
    pub description: String,
    pub orders: Vec<Order>,
}

#[derive(Debug, Serialize, Clone)]
pub struct Order<> {
    pub order_id: i64,
    pub description: String,
    pub customer_id: i64,
    pub items: Vec<Item>,
}

#[derive(Debug, Serialize, Clone)]
pub struct Item {
    pub item_id: i64,
    pub description: String,
    pub order_id: i64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let now = Instant::now();
    dotenv().ok();

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL is not set in .env file");
    let pool = PgPool::new(&database_url).await?;

    let custs = fetch_all_customers(&pool).await;
    let customer_list = match custs {
        Ok(custs) => custs,
        _ => Vec::<Customer>::new()
    };

    let ords = fetch_all_orders(&pool).await;
    let order_list = match ords {
        Ok(ords) => ords,
        _ => Vec::<Order>::new()
    };

    let itms = fetch_all_items(&pool).await;
    let items_list = match itms {
        Ok(itms) => itms,
        _ => Vec::<Item>::new()
    };

    println!("fetched all data after {} milli_sec", now.elapsed().as_millis());

    let mut items_map: HashMap<i64,Vec<Item>> = HashMap::new();
    for item in &items_list {
        items_map.entry(item.order_id).or_insert(Vec::new()).push(item.clone());
    }

    let orders: Vec<Order> = order_list.par_iter().map(|order| sort_data_orders(order.clone(), &items_map)).collect();
    println!("converted orders after {} milli_sec", now.elapsed().as_millis());

    let mut orders_map: HashMap<i64,Vec<Order>> = HashMap::new();
    for order in &orders {
        orders_map.entry(order.customer_id).or_insert(Vec::new()).push(order.clone());
    }

    let customers: Vec<Customer> = customer_list.par_iter().map(|x| sort_data_customers(x.clone(), &orders_map)).collect();
    println!("converted customers after {} sec", now.elapsed().as_millis());

    println!("sorted all data after {} milli_sec", now.elapsed().as_millis());

    let client = Elasticsearch::default();

    println!("{:?}", client.ping());

    bulk_insert_into_el(&client, customers, 2000).await?;

    println!("{}", now.elapsed().as_millis());

    Ok(())
}

fn sort_data_customers(mut customer: Customer, orders_map: &HashMap<i64, Vec<Order>>) -> Customer {
    match orders_map.get(&customer.customer_id) {
        Some(orders) => customer.orders = orders.clone(),
        _ => (),
    }
    customer
}

fn sort_data_orders(mut order: Order, items_map: &HashMap<i64, Vec<Item>>) -> Order {
    match items_map.get(&order.order_id) {
        Some(items) => order.items = items.clone(),
        _ => (),
    } ;
    order
}

async fn bulk_insert_into_el(client: &Elasticsearch, data: Vec<Customer>, size: usize) -> Result<()> {
    let mut body: Vec<JsonBody<_>> = Vec::with_capacity(size);

    for (idx, customer) in data.iter().enumerate() {
        body.push(json!({"index": {"_id": idx}}).into());
        body.push(JsonBody::from(json!(customer)))
    }

    let response = client
        .bulk(BulkParts::Index("customer"))
        .body(body)
        .send()
        .await?;

    let response_body = response.json::<Value>().await?;
    let successful = !response_body["errors"].as_bool().unwrap();

    println!("{}", successful);

    Ok(())
}

async fn fetch_all_customers(pool: &PgPool) -> Result<Vec<Customer>> {
    let mut customers: Vec<Customer> = vec![];

    let recs = sqlx::query!(
        r#"
SELECT customer_id, description
FROM simple.customer
ORDER BY customer_id
        "#
    )
        .fetch_all(pool)
        .await?;

    for rec in recs {
        customers.push(Customer {
            customer_id: rec.customer_id,
            description: rec.description.unwrap(),
            orders: Vec::<Order>::new(),
        }
        )
    }

    Ok(customers)
}

async fn fetch_all_orders(pool: &PgPool) -> Result<Vec<Order>> {
    let mut orders: Vec<Order> = vec![];

    let recs = sqlx::query!(
        r#"
SELECT order_id, order_description, customer_id
FROM simple.order
ORDER BY order_id
        "#
    )
        .fetch_all(pool)
        .await?;

    for rec in recs {
        orders.push(Order {
            order_id: rec.order_id,
            description: rec.order_description.unwrap(),
            customer_id: rec.customer_id.unwrap(),
            items: vec![],
        }
        )
    }

    Ok(orders)
}

async fn fetch_all_items(pool: &PgPool) -> Result<Vec<Item>> {
    let mut items: Vec<Item> = vec![];

    let recs = sqlx::query!(
        r#"
SELECT item_id, item_description, order_id
FROM simple.item
ORDER BY item_id
        "#
    )
        .fetch_all(pool)
        .await?;

    for rec in recs {
        items.push(Item {
            item_id: rec.item_id,
            description: rec.item_description.unwrap(),
            order_id: rec.order_id.unwrap(),
        }
        )
    }

    Ok(items)
}