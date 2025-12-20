use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::*;
use tantivy::{doc, Index, IndexReader, ReloadPolicy, TantivyDocument};
use tantivy_jieba::JiebaTokenizer;

/// 地址查询结果
#[derive(Debug, Clone)]
pub struct AddressResult {
    pub address_code: String,
    pub province: String,
    pub city: String,
    pub district: String,
    pub county: String,
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
    full_address: Field,
    address_code: Field,
}

impl AddressIndex {
    /// 创建新的地址索引
    pub fn new() -> anyhow::Result<Self> {
        println!("正在初始化中文地址索引系统...");

        // 1. 定义 Schema
        // Schema 描述了文档的结构：省市区县字段和地址编码
        let mut schema_builder = Schema::builder();

        // 配置文本字段的索引选项
        // 使用 "jieba" 分词器，并存储词频和位置信息（用于短语查询等）
        // 禁用 FieldNorms，以便我们可以通过重复关键词来提升权重
        let text_indexing = TextFieldIndexing::default()
            .set_tokenizer("jieba")
            .set_index_option(IndexRecordOption::WithFreqsAndPositions)
            .set_fieldnorms(true); // 启用 FieldNorms

        // 设置字段选项：使用上面定义的索引配置，并存储原始文本以便检索时返回
        let text_options = TextOptions::default()
            .set_indexing_options(text_indexing)
            .set_stored();

        // 添加省市区县字段（可搜索、可存储）
        let province = schema_builder.add_text_field("province", text_options.clone());
        let city = schema_builder.add_text_field("city", text_options.clone());
        let district = schema_builder.add_text_field("district", text_options.clone());
        let county = schema_builder.add_text_field("county", text_options.clone());

        // 关键修改：增加完整地址合并列
        let full_address = schema_builder.add_text_field("full_address", text_options.clone());

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
            full_address,
            address_code,
        })
    }

    /// 批量添加地址文档
    pub fn add_documents(
        &self,
        docs: &[(String, String, String, String, String)],
    ) -> anyhow::Result<()> {
        let mut index_writer = self.index.writer(50_000_000)?;
        for (province_val, city_val, district_val, county_val, address_code_val) in docs {
            // 构建完整地址字符串
            // 简单的拼接其实也行，因为我们已经禁用了 fieldnorm
            // 为了更好的搜索体验，我们保留层级结构
            // 使用空格分隔，以便更好地支持分词
            let full = format!(
                "{} {} {} {}",
                province_val, city_val, district_val, county_val
            );

            index_writer.add_document(doc!(
                self.province => province_val.as_str(),
                self.city => city_val.as_str(),
                self.district => district_val.as_str(),
                self.county => county_val.as_str(),
                self.full_address => full,
                self.address_code => address_code_val.as_str()
            ))?;
        }
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
        // 主要针对 full_address 进行搜索
        // 其他字段作为辅助
        let query_parser = QueryParser::for_index(&self.index, vec![self.full_address]);

        // query_parser.set_conjunction_by_default();

        // 不需要单独设置字段 boost，因为现在只搜索 full_address

        query_parser
    }

    /// 预处理查询字符串：分词、去重、用空格连接
    fn preprocess_query(&self, query_str: &str) -> String {
        let mut tokenizer = self.index.tokenizers().get("jieba").unwrap();
        let mut token_stream = tokenizer.token_stream(query_str);
        let mut tokens = Vec::new();
        while token_stream.advance() {
            tokens.push(token_stream.token().text.to_string());
        }

        // 去重
        tokens.sort();
        tokens.dedup();

        tokens.join(" ")
    }

    /// 搜索地址，返回结果字符串数组
    pub fn search_address(&self, query_str: &str) -> anyhow::Result<Vec<String>> {
        let searcher = self.reader.searcher();

        let processed_query = self.preprocess_query(query_str);

        // 使用配置了权重的查询解析器
        let query_parser = self.create_query_parser();
        // 不要强制 AND (set_conjunction_by_default)，因为分词模式可能导致查询词包含索引中不存在的词（如“京市”）
        // 使用默认的 OR 逻辑，配合打分机制筛选结果

        let query = query_parser.parse_query(&processed_query)?;

        // 获取前 10 个匹配结果
        let top_docs = searcher.search(&query, &TopDocs::with_limit(10))?;

        let mut results = Vec::new();
        for (_, doc_address) in top_docs {
            let retrieved_doc: TantivyDocument = searcher.doc(doc_address)?;
            let province_val = retrieved_doc
                .get_first(self.province)
                .and_then(|v| v.as_str())
                .map(|s| s.split_whitespace().next().unwrap_or(s)) // 只取第一个词，去除重复
                .unwrap_or("");
            let city_val = retrieved_doc
                .get_first(self.city)
                .and_then(|v| v.as_str())
                .map(|s| s.split_whitespace().next().unwrap_or(s))
                .unwrap_or("");
            let district_val = retrieved_doc
                .get_first(self.district)
                .and_then(|v| v.as_str())
                .map(|s| s.split_whitespace().next().unwrap_or(s))
                .unwrap_or("");
            let county_val = retrieved_doc
                .get_first(self.county)
                .and_then(|v| v.as_str())
                .map(|s| s.split_whitespace().next().unwrap_or(s))
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
            };

            results.push(result.to_string());
        }

        Ok(results)
    }

    /// 搜索地址的第一个结果，可能为 None
    pub fn search_first(&self, query_str: &str) -> anyhow::Result<Option<AddressResult>> {
        let searcher = self.reader.searcher();

        let processed_query = self.preprocess_query(query_str);

        // 使用配置了权重的查询解析器
        let query_parser = self.create_query_parser();
        let query = query_parser.parse_query(&processed_query)?;

        // 获取第一个匹配结果
        let top_docs = searcher.search(&query, &TopDocs::with_limit(1))?;

        if let Some((_, doc_address)) = top_docs.first() {
            let retrieved_doc: TantivyDocument = searcher.doc(*doc_address)?;
            let province_val = retrieved_doc
                .get_first(self.province)
                .and_then(|v| v.as_str())
                .map(|s| s.split_whitespace().next().unwrap_or(s)) // 只取第一个词，去除重复
                .unwrap_or("");
            let city_val = retrieved_doc
                .get_first(self.city)
                .and_then(|v| v.as_str())
                .map(|s| s.split_whitespace().next().unwrap_or(s))
                .unwrap_or("");
            let district_val = retrieved_doc
                .get_first(self.district)
                .and_then(|v| v.as_str())
                .map(|s| s.split_whitespace().next().unwrap_or(s))
                .unwrap_or("");
            let county_val = retrieved_doc
                .get_first(self.county)
                .and_then(|v| v.as_str())
                .map(|s| s.split_whitespace().next().unwrap_or(s))
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
            }))
        } else {
            Ok(None)
        }
    }
}
