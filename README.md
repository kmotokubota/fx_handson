# マーケットプレイスデータを活用した為替分析Snowflakeハンズオン

Snowflakeハンズオン教材です。マーケットプレイス上の為替データを使った分析と検索アプリケーションを学びます。

## 📁 プロジェクト構成

### 1. fx_handson
為替データ分析のハンズオン教材

**内容:**
- `fx_handson.ipynb` - メインのNotebook（SQL、データ分析、AI分析、Streamlit）
- `fx_handson_app.py` - Streamlit為替分析アプリケーション
- `handson1_setup.sql` - ハンズオン1のセットアップSQL
- `handson2_setup.sql` - ハンズオン2のセットアップSQL

**学習内容:**
- Snowflakeの基本的なSQL操作
- 為替データの取得と分析
- 移動平均などのテクニカル指標の計算
- Snowflake Cortex AI機能（AI_COMPLETE、AI_FILTER）
- Snowflake Pandas API
- Streamlitでの可視化ダッシュボード作成

### 2. simple_search_app
定型検索アプリケーション

**内容:**
- `streamlit_app.py` - メインのStreamlitアプリ
- `pages/1_standard_search.py` - 標準検索ページ
- `setup.sql` - データベースセットアップSQL
- `environment.yml` - Conda環境設定ファイル

**学習内容:**
- Streamlitでの検索UIの実装

## 🚀 使い方

### 前提条件
- Snowflakeアカウント
- Snowflake Notebook環境 または Snowflake上のStreamlit環境

### fx_handson の実行方法

#### 1. Notebookでの実行
1. handson1_setup.sqlを実行
2. MarketplaceからSNOWFLAKE PUBLIC DATA (FREE)をダウンロード
3. セルを順番に実行していく
4. Streamlitアプリを開いて動作を確認

#### 2. Cortex Analystの実行
1. handson2_setup.sqlを実行
2. Snowsightよりエージェントを作成
3. Snowflake CoWorkを実行

### simple_search_app の実行方法

1. `setup.sql`を実行してセットアップ
2. Snowflake上でStreamlitアプリを作成
3. `streamlit_app.py`と`pages/1_standard_search.py`をアップロード
4. アプリを実行



## 📊 データソース

- **FX Data**: `SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.FX_RATES_TIMESERIES`
- Snowflakeの公開データセットを使用

## 🔧 主な技術スタック

- **Snowflake SQL** - データ分析の基礎
- **Snowflake Cortex AI** - AI_COMPLETE、AI_FILTER
- **Snowflake Pandas API** - pandas互換のデータ処理
- **Streamlit** - データアプリケーション開発
- **Plotly** - インタラクティブな可視化

## 📚 参考リソース

- [Snowflake Documentation](https://docs.snowflake.com/)
- [Snowflake Cortex AI Functions](https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions)
- [Snowflake Pandas API](https://docs.snowflake.com/ja/developer-guide/snowpark/python/pandas-on-snowflake)
- [Streamlit Documentation](https://docs.streamlit.io/)

---


