mod address_index;
mod csv_loader;

use address_index::AddressIndex;
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

fn build_index(address_index: &AddressIndex) -> anyhow::Result<()> {
    // 加载 CSV 数据
    println!("正在加载 CSV 数据...");
    let csv_path = "./src/areas.csv"; // 假设文件名为 areas.csv
    let regions = load_regions(csv_path)?;

    println!("读取到 {} 条记录，正在构建索引...", regions.len());
    let region_map = build_region_map(&regions);

    // 批量处理以提高性能
    let mut docs = Vec::with_capacity(regions.len());
    for region in &regions {
        let (province, city, district, county) = resolve_address(region, &region_map);

        docs.push((province, city, district, county, region.ext_id.clone()));
    }
    address_index.add_documents(&docs)?;
    println!("索引构建完成！");

    Ok(())
}

fn main() -> anyhow::Result<()> {
    // 创建地址索引（使用默认权重）
    let mut address_index = AddressIndex::new()?;

    if let Err(e) = build_index(&address_index) {
        panic!("加载 CSV 失败: {}", e);
    }

    // 提交更改并重新加载索引
    address_index.commit()?;

    // 执行搜索，返回字符串数组
    let query = "兴宁市";
    let results = address_index.search_address(query)?;
    println!("找到 {} 条结果:", results.len());
    for result in &results {
        println!("{}", result);
    }

    // 搜索第一个结果
    if let Some(first) = address_index.search_first(query)? {
        println!("\n第一个结果: {}", first.to_string());
    } else {
        println!("\n未找到匹配结果");
    }

    Ok(())
}
