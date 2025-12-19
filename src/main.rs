mod address_index;
use address_index::AddressIndex;

fn main() -> anyhow::Result<()> {
    // 创建地址索引（使用默认权重）
    let mut address_index = AddressIndex::new()?;

    // 或者使用自定义权重配置
    // let custom_weights = FieldWeights {
    //     province: 1.0,
    //     city: 3.0,
    //     district: 6.0,
    //     county: 10.0,
    // };
    // let mut address_index = AddressIndex::with_weights(custom_weights)?;

    // 装载数据
    println!("正在装载数据...");
    address_index.add_document("北京市", "", "", "", "110105")?;
    address_index.add_document("北京市", "北京市", "", "", "110105")?;
    address_index.add_document("北京市", "北京市", "朝阳区", "", "110105")?;
    address_index.add_document("上海市", "上海市", "浦东新区", "", "310115")?;
    address_index.add_document("北京市", "北京市", "海淀区", "", "110108")?;

    // 提交更改并重新加载索引
    address_index.commit()?;

    // 执行搜索，返回字符串数组
    let results = address_index.search_address("北京市")?;
    println!("找到 {} 条结果:", results.len());
    for result in &results {
        println!("{}", result);
    }

    // 搜索第一个结果
    if let Some(first) = address_index.search_first("北京市")? {
        println!("\n第一个结果: {}", first.to_string());
    } else {
        println!("\n未找到匹配结果");
    }

    Ok(())
}
