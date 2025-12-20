use serde::Deserialize;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;

#[derive(Debug, Deserialize, Clone)]
pub struct Region {
    pub id: u64,
    pub pid: u64,
    pub deep: u8,
    #[allow(dead_code)]
    pub name: String,
    #[allow(dead_code)]
    pub pinyin_prefix: String,
    #[allow(dead_code)]
    pub pinyin: String,
    pub ext_id: String,
    pub ext_name: String,
}

pub fn load_regions<P: AsRef<Path>>(path: P) -> anyhow::Result<Vec<Region>> {
    let file = File::open(path)?;
    let mut rdr = csv::Reader::from_reader(file);
    let mut regions = Vec::new();
    for result in rdr.deserialize() {
        let record: Region = result?;
        regions.push(record);
    }
    Ok(regions)
}

pub fn build_region_map(regions: &[Region]) -> HashMap<u64, Region> {
    regions.iter().map(|r| (r.id, r.clone())).collect()
}
