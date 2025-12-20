use std::env;
use std::fs;
use std::path::Path;
use tantivy::schema::*;
use tantivy::{doc, Index};
use tantivy_jieba::JiebaTokenizer;

// Include the csv_loader module
#[path = "src/csv_loader.rs"]
mod csv_loader;
use csv_loader::{build_region_map, load_regions, Region};
use std::collections::HashMap;

fn resolve_address(
    region: &Region,
    map: &HashMap<u64, Region>,
) -> (String, String, String, String) {
    let mut province = String::new();
    let mut city = String::new();
    let mut district = String::new();
    let mut county = String::new();

    let mut current = Some(region);
    while let Some(r) = current {
        match r.deep {
            0 => province = r.ext_name.clone(),
            1 => city = r.ext_name.clone(),
            2 => district = r.ext_name.clone(),
            3 => county = r.ext_name.clone(),
            _ => {} // Ignore deeper levels if any
        }

        if r.pid == 0 {
            break;
        }
        current = map.get(&r.pid);
    }

    (province, city, district, county)
}

fn main() -> anyhow::Result<()> {
    // 1. Setup paths
    let out_dir = env::var("OUT_DIR")?;
    let dest_path = Path::new(&out_dir).join("index_loader.rs");
    let index_dir = Path::new(&out_dir).join("text2location_index");

    // Ensure clean index directory
    if index_dir.exists() {
        fs::remove_dir_all(&index_dir)?;
    }
    fs::create_dir_all(&index_dir)?;

    println!("cargo:rerun-if-changed=src/areas.csv");
    println!("cargo:rerun-if-changed=src/csv_loader.rs");

    // 2. Define Schema (must match src/address_index.rs)
    let mut schema_builder = Schema::builder();

    let text_indexing = TextFieldIndexing::default()
        .set_tokenizer("jieba")
        .set_index_option(IndexRecordOption::WithFreqsAndPositions)
        .set_fieldnorms(true);

    let text_options = TextOptions::default()
        .set_indexing_options(text_indexing)
        .set_stored();

    let province_field = schema_builder.add_text_field("province", text_options.clone());
    let city_field = schema_builder.add_text_field("city", text_options.clone());
    let district_field = schema_builder.add_text_field("district", text_options.clone());
    let county_field = schema_builder.add_text_field("county", text_options.clone());
    let full_address_field = schema_builder.add_text_field("full_address", text_options.clone());
    let address_code_field = schema_builder.add_text_field("address_code", STRING | STORED);

    let schema = schema_builder.build();

    // 3. Create Index
    let index = Index::create_in_dir(&index_dir, schema)?;
    let tokenizer = JiebaTokenizer {};
    index.tokenizers().register("jieba", tokenizer);

    // 4. Load Data
    let csv_path = "src/areas.csv";
    let regions = load_regions(csv_path)?;
    let region_map = build_region_map(&regions);

    let mut index_writer = index.writer(50_000_000)?;

    for region in &regions {
        let (province, city, district, county) = resolve_address(region, &region_map);
        let full = format!("{} {} {} {}", province, city, district, county);

        index_writer.add_document(doc!(
            province_field => province,
            city_field => city,
            district_field => district,
            county_field => county,
            full_address_field => full,
            address_code_field => region.ext_id.clone()
        ))?;
    }

    index_writer.commit()?;

    // Explicitly drop writer and index to ensure locks are released and files flushed
    drop(index_writer);
    drop(index);

    // 5. Generate Rust code to embed the index
    let mut code = String::new();
    code.push_str("use tantivy::directory::RamDirectory;\n");
    code.push_str("use std::io::Write;\n\n");
    code.push_str("pub fn load_index_directory() -> anyhow::Result<RamDirectory> {\n");
    code.push_str("    let dir = RamDirectory::create();\n");

    for entry in fs::read_dir(&index_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            let filename = path.file_name().unwrap().to_str().unwrap();
            // Don't include lock file if any (though tantivy usually cleans up)
            if filename.ends_with(".lock") {
                continue;
            }

            code.push_str("    {\n");
            code.push_str(&format!(
                "        let data = include_bytes!(r\"{}\");\n",
                path.display()
            ));
            code.push_str(&format!(
                "        let mut write = dir.open_write(Path::new(\"{}\"))?;\n",
                filename
            ));
            code.push_str("        write.write_all(data)?;\n");
            code.push_str("        write.terminate()?;\n");
            code.push_str("    }\n");
        }
    }

    code.push_str("    Ok(dir)\n");
    code.push_str("}\n");

    fs::write(&dest_path, code)?;

    Ok(())
}
