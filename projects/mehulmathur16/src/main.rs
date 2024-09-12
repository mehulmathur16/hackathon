use actix_web::{guard, web, App, HttpServer};
use async_graphql::dataloader::{DataLoader, Loader};
use async_graphql::Error;
use async_graphql::{Context, EmptyMutation, EmptySubscription, Object, Schema, SimpleObject};
use async_graphql_actix_web::{GraphQLRequest, GraphQLResponse};
use reqwest::Client;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize, Clone)]
struct Post {
    id: i32,
    #[serde(rename = "userId")]
    user_id: i32,
    title: String,
    body: String,
}

#[derive(SimpleObject, Deserialize, Clone)]
struct User {
    id: i32,
    name: String,
    username: String,
    email: String,
    address: Address,
    phone: String,
    website: String,
}

#[derive(SimpleObject, Deserialize, Clone)]
struct Address {
    zipcode: String,
    geo: Geo,
}

#[derive(SimpleObject, Deserialize, Clone)]
struct Geo {
    lat: f64,
    lng: f64,
}

struct UserLoader {
    client: Client,
}

#[async_trait::async_trait]
impl Loader<i32> for UserLoader {
    type Value = User;
    type Error = Error;

    async fn load(&self, keys: &[i32]) -> Result<HashMap<i32, Self::Value>, Self::Error> {
        let ids = keys
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("&id=");

        let response = self
            .client
            .get(&format!("http://localhost:3000/users?id={}", ids))
            .send()
            .await?
            .json::<Vec<User>>()
            .await?;

        let mut user_map = HashMap::new();
        for user in response {
            user_map.insert(user.id, user);
        }
        Ok(user_map)
    }
}

struct PostsLoader {
    client: Client,
}

#[async_trait::async_trait]
impl Loader<i32> for PostsLoader {
    type Value = Post;
    type Error = Error;

    async fn load(&self, keys: &[i32]) -> Result<HashMap<i32, Self::Value>, Self::Error> {
        let futures = keys.iter().map(|&id| {
            let client = self.client.clone();
            async move {
                let response = client
                    .get(&format!("http://localhost:3000/posts/{}", id))
                    .send()
                    .await;

                match response {
                    Ok(res) => match res.json::<Post>().await {
                        Ok(post) => Some((id, post)),
                        Err(_) => None,
                    },
                    Err(_) => None,
                }
            }
        });

        let futures: Vec<_> = futures.collect();

        let results = futures::future::join_all(futures).await;

        let mut post_map = HashMap::new();
        for result in results {
            if let Some((id, post)) = result {
                post_map.insert(id, post);
            }
        }

        Ok(post_map)
    }
}

struct QueryRoot;

#[Object]
impl QueryRoot {
    async fn posts(&self, ctx: &Context<'_>) -> async_graphql::Result<Vec<Post>> {
        let client = ctx.data::<Client>().unwrap();
        let response = client
            .get("http://localhost:3000/posts")
            .send()
            .await?
            .json::<Vec<Post>>()
            .await?;
        Ok(response)
    }

    async fn post(&self, ctx: &Context<'_>, id: i32) -> async_graphql::Result<Post> {
        let loader = ctx.data::<DataLoader<PostsLoader>>().unwrap();
        loader
            .load_one(id)
            .await?
            .ok_or_else(|| async_graphql::Error::new("Post not found"))
    }

    async fn users(&self, ctx: &Context<'_>) -> async_graphql::Result<Vec<User>> {
        let client = ctx.data::<Client>().unwrap();
        let response = client
            .get("http://localhost:3000/users")
            .send()
            .await?
            .json::<Vec<User>>()
            .await?;
        Ok(response)
    }

    async fn user(&self, ctx: &Context<'_>, id: i32) -> async_graphql::Result<User> {
        let client = ctx.data::<Client>().unwrap();
        let response = client
            .get(&format!("http://localhost:3000/users/{}", id))
            .send()
            .await?
            .json::<User>()
            .await?;
        Ok(response)
    }
}

#[Object]
impl Post {
    async fn id(&self) -> i32 {
        self.id
    }

    async fn title(&self) -> &str {
        &self.title
    }

    async fn body(&self) -> &str {
        &self.body
    }

    async fn user(&self, ctx: &Context<'_>, id: i32) -> async_graphql::Result<User> {
        let loader = ctx.data::<DataLoader<UserLoader>>().unwrap();
        let user = loader
            .load_one(id)
            .await?
            .ok_or_else(|| async_graphql::Error::new("User not found"))?;
        Ok(user)
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let client = Client::new();
    let user_loader = DataLoader::new(
        UserLoader {
            client: client.clone(),
        },
        tokio::spawn,
    );

    let posts_loader = DataLoader::new(
        PostsLoader {
            client: client.clone(),
        },
        tokio::spawn,
    );

    let schema = Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
        .data(client)
        .data(user_loader)
        .data(posts_loader)
        .finish();

    HttpServer::new(move || {
        App::new().app_data(web::Data::new(schema.clone())).service(
            web::resource("/graphql")
                .guard(guard::Post())
                .to(graphql_handler),
        )
    })
    .bind("0.0.0.0:8000")?
    .run()
    .await
}

async fn graphql_handler(
    schema: web::Data<Schema<QueryRoot, EmptyMutation, EmptySubscription>>,
    req: GraphQLRequest,
) -> GraphQLResponse {
    schema.execute(req.into_inner()).await.into()
}
