use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::*;
use tantivy::{doc, Index, IndexReader, ReloadPolicy, TantivyDocument};
use tantivy_jieba::JiebaTokenizer;

/// 字段权重配置
#[derive(Debug, Clone)]
pub struct FieldWeights {
    pub province: f32,
    pub city: f32,
    pub district: f32,
    pub county: f32,
}

impl Default for FieldWeights {
    fn default() -> Self {
        Self {
            province: 1.0, // 省权重最低
            city: 2.0,     // 市权重稍高
            district: 4.0, // 区权重更高
            county: 8.0,   // 县权重最高
        }
    }
}

/// 地址查询结果
#[derive(Debug, Clone)]
pub struct AddressResult {
    pub address_code: String,
    pub province: String,
    pub city: String,
    pub district: String,
    pub county: String,
    pub score: f32,
}

impl AddressResult {
    /// 格式化为字符串
    pub fn to_string(&self) -> String {
        format!(
            "编码: {} | 省: {} | 市: {} | 区: {} | 县: {}",
            self.address_code, self.province, self.city, self.district, self.county
        )
    }
}

/// 地址索引结构体，封装索引和查询功能
pub struct AddressIndex {
    index: Index,
    reader: IndexReader,
    province: Field,
    city: Field,
    district: Field,
    county: Field,
    address_code: Field,
    weights: FieldWeights,
}

impl AddressIndex {
    /// 创建新的地址索引，使用默认权重配置
    pub fn new() -> anyhow::Result<Self> {
        Self::with_weights(FieldWeights::default())
    }

    /// 创建新的地址索引，使用自定义权重配置
    pub fn with_weights(weights: FieldWeights) -> anyhow::Result<Self> {
        println!("正在初始化中文地址索引系统...");

        // 1. 定义 Schema
        // Schema 描述了文档的结构：省市区县字段和地址编码
        let mut schema_builder = Schema::builder();

        // 配置文本字段的索引选项
        // 使用 "jieba" 分词器，并存储词频和位置信息（用于短语查询等）
        let text_indexing = TextFieldIndexing::default()
            .set_tokenizer("jieba")
            .set_index_option(IndexRecordOption::WithFreqsAndPositions);

        // 设置字段选项：使用上面定义的索引配置，并存储原始文本以便检索时返回
        let text_options = TextOptions::default()
            .set_indexing_options(text_indexing)
            .set_stored();

        // 添加省市区县字段（可搜索、可存储）
        let province = schema_builder.add_text_field("province", text_options.clone());
        let city = schema_builder.add_text_field("city", text_options.clone());
        let district = schema_builder.add_text_field("district", text_options.clone());
        let county = schema_builder.add_text_field("county", text_options.clone());

        // 地址编码字段（仅存储，不索引，用于唯一标识）
        let address_code = schema_builder.add_text_field("address_code", STRING | STORED);

        let schema = schema_builder.build();

        // 2. 创建索引 (在内存中)
        // 实际生产环境可以使用 Index::create_in_dir 在磁盘创建索引
        let index = Index::create_in_ram(schema.clone());

        // 3. 注册 Jieba 分词器
        // 这是关键步骤，让 tantivy 知道如何处理中文
        let tokenizer = JiebaTokenizer {};
        index.tokenizers().register("jieba", tokenizer);

        // 4. 创建 Reader
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;

        Ok(Self {
            index,
            reader,
            province,
            city,
            district,
            county,
            address_code,
            weights,
        })
    }

    /// 添加地址文档
    pub fn add_document(
        &self,
        province_val: &str,
        city_val: &str,
        district_val: &str,
        county_val: &str,
        address_code_val: &str,
    ) -> anyhow::Result<()> {
        let mut index_writer = self.index.writer(50_000_000)?;
        index_writer.add_document(doc!(
            self.province => province_val,
            self.city => city_val,
            self.district => district_val,
            self.county => county_val,
            self.address_code => address_code_val
        ))?;
        index_writer.commit()?;
        Ok(())
    }

    /// 提交更改并重新加载索引
    pub fn commit(&mut self) -> anyhow::Result<()> {
        self.reader.reload()?;
        Ok(())
    }

    /// 创建配置了字段权重的 QueryParser
    /// 使用配置的权重值
    fn create_query_parser(&self) -> QueryParser {
        let mut query_parser = QueryParser::for_index(
            &self.index,
            vec![self.province, self.city, self.district, self.county],
        );

        // 使用配置的字段权重
        query_parser.set_field_boost(self.province, self.weights.province);
        query_parser.set_field_boost(self.city, self.weights.city);
        query_parser.set_field_boost(self.district, self.weights.district);
        query_parser.set_field_boost(self.county, self.weights.county);

        query_parser
    }

    /// 更新字段权重配置
    pub fn set_weights(&mut self, weights: FieldWeights) {
        self.weights = weights;
    }

    /// 获取当前字段权重配置
    pub fn get_weights(&self) -> &FieldWeights {
        &self.weights
    }

    /// 搜索地址，返回结果字符串数组
    pub fn search_address(&self, query_str: &str) -> anyhow::Result<Vec<String>> {
        let searcher = self.reader.searcher();

        // 使用配置了权重的查询解析器
        let query_parser = self.create_query_parser();
        let query = query_parser.parse_query(query_str)?;

        // 获取前 10 个匹配结果
        let top_docs = searcher.search(&query, &TopDocs::with_limit(10))?;

        let mut results = Vec::new();
        for (score, doc_address) in top_docs {
            let retrieved_doc: TantivyDocument = searcher.doc(doc_address)?;
            let province_val = retrieved_doc
                .get_first(self.province)
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let city_val = retrieved_doc
                .get_first(self.city)
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let district_val = retrieved_doc
                .get_first(self.district)
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let county_val = retrieved_doc
                .get_first(self.county)
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let address_code_val = retrieved_doc
                .get_first(self.address_code)
                .and_then(|v| v.as_str())
                .unwrap_or("");

            let result = AddressResult {
                address_code: address_code_val.to_string(),
                province: province_val.to_string(),
                city: city_val.to_string(),
                district: district_val.to_string(),
                county: county_val.to_string(),
                score,
            };

            results.push(result.to_string());
        }

        Ok(results)
    }

    /// 搜索地址的第一个结果，可能为 None
    pub fn search_first(&self, query_str: &str) -> anyhow::Result<Option<AddressResult>> {
        let searcher = self.reader.searcher();

        // 使用配置了权重的查询解析器
        let query_parser = self.create_query_parser();
        let query = query_parser.parse_query(query_str)?;

        // 获取第一个匹配结果
        let top_docs = searcher.search(&query, &TopDocs::with_limit(1))?;

        if let Some((score, doc_address)) = top_docs.first() {
            let retrieved_doc: TantivyDocument = searcher.doc(*doc_address)?;
            let province_val = retrieved_doc
                .get_first(self.province)
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let city_val = retrieved_doc
                .get_first(self.city)
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let district_val = retrieved_doc
                .get_first(self.district)
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let county_val = retrieved_doc
                .get_first(self.county)
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let address_code_val = retrieved_doc
                .get_first(self.address_code)
                .and_then(|v| v.as_str())
                .unwrap_or("");

            Ok(Some(AddressResult {
                address_code: address_code_val.to_string(),
                province: province_val.to_string(),
                city: city_val.to_string(),
                district: district_val.to_string(),
                county: county_val.to_string(),
                score: *score,
            }))
        } else {
            Ok(None)
        }
    }
}
