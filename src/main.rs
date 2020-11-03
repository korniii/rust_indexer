use dotenv::dotenv;
use sqlx::{PgPool};
use std::env;
use anyhow::Result;
use elasticsearch::{Elasticsearch, BulkParts};
use serde_json::{json, Value};
use serde::{Serialize};
use elasticsearch::http::request::JsonBody;
use std::time::Instant;


#[derive(Debug, Serialize)]
pub struct Customer<'a> {
    pub id: i64,
    pub description: String,
    pub orders: Vec<&'a Order<'a>>
}

#[derive(Debug, Serialize)]
pub struct Order<'a> {
    pub id: i64,
    pub description: String,
    pub customer_id: i64,
    pub items: Vec<&'a Item>
}

#[derive(Debug, Serialize)]
pub struct Item {
    pub id: i64,
    pub description: String,
    pub order_id: i64
}

#[tokio::main]
async fn main() -> Result<()>{
    let now = Instant::now();
    dotenv().ok();

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL is not set in .env file");
    let pool = PgPool::new(&database_url).await?;

    let custs = fetch_all_customers(&pool).await;
    let mut customer_list = match custs {
        Ok(custs) => custs,
        _ => Vec::<Customer>::new() 
    };

    let ords = fetch_all_orders(&pool).await;
    let mut order_list = match ords {
        Ok(ords) => ords,
        _ => Vec::<Order>::new()
    };

    let itms = fetch_all_items(&pool).await;
    let items_list = match itms {
        Ok(itms) => itms,
        _ => Vec::<Item>::new()
    };

    println!("fetched all data after {} sec", now.elapsed().as_millis());

    for order in &mut order_list {
        let id = order.id;
        for item in &items_list {
            if id == item.order_id {
                order.items.push(item);
            }
        }
    }

    println!("converted orders after {} sec", now.elapsed().as_millis());

    for customer in &mut customer_list {
        let id = customer.id;
        for order in &order_list {
            if id == order.customer_id {
               customer.orders.push(order);
            }
        }
    }

    println!("converted customers after {} sec", now.elapsed().as_millis());

    let client = Elasticsearch::default();

    println!("{:?}", client.ping());

    bulk_insert_into_el(&client, customer_list,2000).await?;

    println!("{}", now.elapsed().as_millis());

    Ok(())
}

async fn bulk_insert_into_el(client: &Elasticsearch, data: Vec<Customer<'_>>,size: usize) -> Result<()> {
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

async fn fetch_all_customers(pool: &PgPool) -> Result<Vec<Customer<'_>>> {
    let mut customers: Vec<Customer> = vec![];

    let recs = sqlx::query!(
        r#"
SELECT id, description
FROM simple.customer
ORDER BY id
        "#
    )
    .fetch_all(pool)
    .await?;

    for rec in recs {
        customers.push(Customer {
            id: rec.id,
            description: rec.description.unwrap(),
            orders: Vec::<&Order>::new()
        }
        )
    }

    Ok(customers)
}

async fn fetch_all_orders(pool: &PgPool) -> Result<Vec<Order<'_>>> {
    let mut orders: Vec<Order> = vec![];

    let recs = sqlx::query!(
        r#"
SELECT id, order_description, customer_id
FROM simple.order
ORDER BY id
        "#
    )
        .fetch_all(pool)
        .await?;

    for rec in recs {
        orders.push(Order {
            id: rec.id,
            description: rec.order_description.unwrap(),
            customer_id: rec.customer_id.unwrap(),
            items: vec![]
        }
        )
    }

    Ok(orders)
}

async fn fetch_all_items(pool: &PgPool) -> Result<Vec<Item>> {
    let mut items: Vec<Item> = vec![];

    let recs = sqlx::query!(
        r#"
SELECT id, item_description, order_id
FROM simple.item
ORDER BY id
        "#
    )
        .fetch_all(pool)
        .await?;

    for rec in recs {
        items.push(Item {
            id: rec.id,
            description: rec.item_description.unwrap(),
            order_id: rec.order_id.unwrap()
        }
        )
    }

    Ok(items)
}