use serde::{Deserialize, Serialize};
use std::error::Error;
use surrealdb::engine::local::{Db, Mem};
use surrealdb::sql::Thing;
use surrealdb::Surreal;

pub trait IdTraits {
    fn get_tbl_id(&self) -> String;
    fn get_id(&self) -> String;
    fn get_tbl(&self) -> String;
}

impl IdTraits for Thing {
    // Note the `⟨` and `⟩` in the `id` field. This is because the `id` field
    // is a `Thing` and if the `thing.id` field is Decimal Number than those
    // characters surround the id. And those aren't the '<' and '>' characters!
    fn get_tbl_id(&self) -> String {
        self.to_raw()

        // This will not have the surrounding `⟨` and `⟩` characters on Numbers
        //self.get_tbl() + ":" + &self.get_id()
    }

    fn get_id(&self) -> String {
        self.id.to_raw()
    }

    fn get_tbl(&self) -> String {
        self.tb.to_string()
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct BuildingWithThing {
    id: Thing,
    address: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct BuildingWithRidString {
    rid: String,
    address: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct BuildingWithRidOptionString {
    rid: Option<String>,
    address: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct Building {
    address: String,
}

#[allow(unused)]
async fn test_select(db: &Surreal<Db>, address: &str) -> Result<(), Box<dyn Error>> {
    // Using `db.select` always returns None for `rid: Option<String>` field
    let select_results_with_rid_option_string: Vec<BuildingWithRidOptionString> =
        db.select("building_tbl").await?;
    dbg!(&select_results_with_rid_option_string);
    assert_eq!(select_results_with_rid_option_string.len(), 1);
    assert_eq!(select_results_with_rid_option_string[0].rid, None);
    assert_eq!(select_results_with_rid_option_string[0].address, address);

    // Using `db.select` with no `rid` works too
    let select_results_no_rid: Vec<Building> = db.select("building_tbl").await?;
    dbg!(&select_results_no_rid);
    assert_eq!(select_results_no_rid.len(), 1);
    assert_eq!(select_results_no_rid[0].address, address);

    // Having `rid: String` always fails when using select.
    match db.select("building_tbl").await {
        Ok(r) => {
            let _: Vec<BuildingWithRidString> = r;
            panic!("Should have failed");
        }
        Err(e) => {
            //dbg!(&e);
            assert!(e.to_string().contains("missing field"));
        }
    }

    Ok(())
}

#[allow(unused)]
async fn test_query(db: &Surreal<Db>, address: &str, rid: &str) -> Result<(), Box<dyn Error>> {
    // Query all fields without the meta::id as rid
    let mut query_response_no_rid = db.query(r#"SELECT * FROM building_tbl"#).await?;
    dbg!(&query_response_no_rid);
    let query_results_no_rid: Vec<BuildingWithRidOptionString> = query_response_no_rid.take(0)?;
    dbg!(&query_results_no_rid);
    assert_eq!(query_results_no_rid.len(), 1);
    assert_eq!(query_results_no_rid[0].rid, None);
    assert_eq!(query_results_no_rid[0].address, address);

    // Query all fields and the meta::id as rid
    let mut query_response_with_rid = db
        .query(r#"SELECT *,meta::id(id) AS rid FROM building_tbl"#)
        .await?;
    dbg!(&query_response_with_rid);
    let query_results_with_rid: Vec<BuildingWithRidOptionString> =
        query_response_with_rid.take(0)?;
    dbg!(&query_results_with_rid);
    assert_eq!(query_results_with_rid.len(), 1);
    assert_eq!(query_results_with_rid[0].rid, Some(rid.to_owned()));
    assert_eq!(query_results_with_rid[0].address, address);

    // Query without `meta::id as rid` and no field rid: Option<String>
    let mut query_response_no_rid_in_query_or_struct =
        db.query(r#"SELECT * FROM building_tbl"#).await?;
    dbg!(&query_response_no_rid_in_query_or_struct);
    let query_results_no_rid_in_query_or_struct: Vec<Building> =
        query_response_no_rid_in_query_or_struct.take(0)?;
    dbg!(&query_results_no_rid_in_query_or_struct);
    assert_eq!(query_results_no_rid_in_query_or_struct.len(), 1);
    assert_eq!(query_results_no_rid_in_query_or_struct[0].address, address);

    // Query with `meta::id(id) as rid` but no field `rid: Option<String>`
    let mut query_response_rid_in_query_no_rid_in_struct = db
        .query(r#"SELECT *,meta::id(id) AS rid FROM building_tbl"#)
        .await?;
    dbg!(&query_response_rid_in_query_no_rid_in_struct);
    let query_results_rid_in_query_no_rid_in_struct: Vec<Building> =
        query_response_rid_in_query_no_rid_in_struct.take(0)?;
    dbg!(&query_results_rid_in_query_no_rid_in_struct);
    assert_eq!(query_results_rid_in_query_no_rid_in_struct.len(), 1);
    assert_eq!(
        query_results_rid_in_query_no_rid_in_struct[0].address,
        address
    );

    Ok(())
}

async fn test_select_thing_with_id_traits(db: &Surreal<Db>, address: &str, tbl: &str, id: &str) -> Result<(), Box<dyn Error>> {
    let select_results_with_id_traits: Vec<BuildingWithThing> = db.select("building_tbl").await?;
    dbg!(&select_results_with_id_traits);
    assert_eq!(select_results_with_id_traits.len(), 1);
    assert_eq!(&select_results_with_id_traits[0].id.get_tbl(), tbl);
    assert_eq!(&select_results_with_id_traits[0].id.get_id(), id);
    // Note the `⟨` and `⟩` in the `id` field. This is because the `id` field is a `Thing` and the `id` field is a `meta::id` field.
    // and those aren't the '<' and '>' characters!
    assert_eq!(&select_results_with_id_traits[0].id.get_tbl_id(), &(tbl.to_owned() + ":⟨" + id + "⟩"));
    assert_eq!(&select_results_with_id_traits[0].address, address);

    println!("id: {:?}", select_results_with_id_traits[0].id);
    println!("id.id: {:?}", select_results_with_id_traits[0].id.id);
    let id_get_tbl_id = select_results_with_id_traits[0].id.get_tbl_id();
    println!("id_get_tbl_id: {}", id_get_tbl_id);
    let id_get_id = select_results_with_id_traits[0].id.get_id();
    println!("id_get_id: {}", id_get_id);
    let id_get_tbl = select_results_with_id_traits[0].id.get_tbl();
    println!("id_get_tbl: {}", id_get_tbl);

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Create a new SurrealDB instance
    let db = Surreal::new::<Mem>(()).await?;
    dbg!(&db);

    db.use_ns("test").use_db("test").await?;

    // Add a record with a `rid` and `address` fields
    let table = "building_tbl";
    let address = "123 Main St";
    let rid = "1234567890";
    let created_response = db
        .query(r#"CREATE building_tbl SET id = $rid, address = $addr;"#)
        //.bind(("table", table))
        .bind(("rid", rid))
        .bind(("addr", address))
        .await?;
    dbg!(created_response);

    test_select(&db, address).await?;
    test_query(&db, address, rid).await?;
    test_select_thing_with_id_traits(&db, address, table, rid).await?;

    Ok(())
}
