# =========================================================
# Snowflakeデータ操作アプリケーション
# 定型検索ページ
# =========================================================
# Created by kdaigo
# 最終更新: 2025/09/24
# =========================================================

import streamlit as st
import pandas as pd
import json
import time
from datetime import datetime, timedelta
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, lit
import uuid

# ページ設定
st.set_page_config(
    layout="wide",
    page_title="🔍 定型検索",
    page_icon="🔍"
)

# Snowflakeセッション取得
@st.cache_resource
def get_snowflake_session():
    return get_active_session()

session = get_snowflake_session()

# =========================================================
# セッション状態の初期化
# =========================================================
if 'new_selected_columns_state' not in st.session_state:
    st.session_state.new_selected_columns_state = set()
if 'last_result_df' not in st.session_state:
    st.session_state.last_result_df = None
if 'where_conditions_list' not in st.session_state:
    st.session_state.where_conditions_list = []
if 'order_by_conditions_list' not in st.session_state:
    st.session_state.order_by_conditions_list = []
if 'favorites' not in st.session_state:
    st.session_state.favorites = []
if 'execute_query_request' not in st.session_state:
    st.session_state.execute_query_request = None
if 'date_condition' not in st.session_state:
    st.session_state.date_condition = {}

# =========================================================
# ユーティリティ関数
# =========================================================
# テーブル作成関数は削除 - setup SQLで事前作成済み

def load_standard_search_objects():
    try:
        result = session.sql("SELECT * FROM application_db.application_schema.STANDARD_SEARCH_OBJECTS ORDER BY created_at DESC").collect()
        return [row.as_dict() for row in result]
    except:
        return []

def save_standard_search_object(object_data: dict):
    """定型検索オブジェクトを保存"""
    try:
        session.sql("""
        INSERT INTO application_db.application_schema.STANDARD_SEARCH_OBJECTS (
            object_id, object_name, description, search_query
        ) VALUES (?, ?, ?, ?)
        """, params=[
            object_data['object_id'],
            object_data['object_name'],
            object_data['description'],
            object_data['search_query']
        ]).collect()
        return True
    except Exception as e:
        st.error(f"保存エラー: {str(e)}")
        return False

def execute_standard_search(object_id: str):
    try:
        result = session.sql("SELECT * FROM application_db.application_schema.STANDARD_SEARCH_OBJECTS WHERE object_id = ?", params=[object_id]).collect()
        if not result:
            return False, "検索オブジェクトが見つかりません"
        search_obj = result[0].as_dict()
        search_query = search_obj['SEARCH_QUERY']
        search_result = session.sql(search_query).collect()
        session.sql("""
        UPDATE application_db.application_schema.STANDARD_SEARCH_OBJECTS 
        SET execution_count = execution_count + 1, 
            last_executed = CURRENT_TIMESTAMP()
        WHERE object_id = ?
        """, params=[object_id]).collect()
        return True, search_result
    except Exception as e:
        return False, str(e)

def update_execution_count(object_id: str):
    """実行回数を更新する専用関数"""
    try:
        session.sql("""
        UPDATE application_db.application_schema.STANDARD_SEARCH_OBJECTS 
        SET execution_count = execution_count + 1, 
            last_executed = CURRENT_TIMESTAMP()
        WHERE object_id = ?
        """, params=[object_id]).collect()
        return True
    except Exception as e:
        st.error(f"実行回数更新エラー: {str(e)}")
        return False


def add_to_favorites(object_id: str):
    try:
        session.sql("""
        UPDATE application_db.application_schema.STANDARD_SEARCH_OBJECTS 
        SET is_favorite = TRUE 
        WHERE object_id = ?
        """, params=[object_id]).collect()
        return True
    except:
        return False

# =========================================================
# ユーティリティ関数（キャッシュ対応）
# =========================================================
@st.cache_data(ttl=300, show_spinner=False)
def get_available_relations():
    """同一スキーマ内のテーブルとビュー名を取得（5分キャッシュ）"""
    try:
        # テーブル一覧を取得
        table_result = session.sql("SHOW TABLES").collect()
        tables = [row['name'] for row in table_result]
    except Exception as e:
        st.error(f"テーブル取得エラー: {str(e)}")
        tables = []
    
    try:
        # ビュー一覧を取得
        view_result = session.sql("SHOW VIEWS").collect()
        views = [row['name'] for row in view_result]
    except Exception as e:
        st.error(f"ビュー取得エラー: {str(e)}")
        views = []
    
    labeled = [f"[TABLE] {t}" for t in tables] + [f"[VIEW] {v}" for v in views]
    return sorted(labeled)

@st.cache_data(ttl=300, show_spinner=False)
def get_table_columns_with_types_cached(table_name: str):
    """テーブル/ビューのカラム名とデータ型を取得（5分キャッシュ）"""
    try:
        # 日本語テーブル名に対応するためダブルクォーテーションで囲む
        quoted_table_name = f'"{table_name}"' if not table_name.startswith('"') else table_name
        result = session.sql(f"DESCRIBE TABLE {quoted_table_name}").collect()
        return [{'name': row['name'], 'type': row['type']} for row in result]
    except Exception as e:
        st.error(f"テーブル情報取得エラー ({table_name}): {str(e)}")
        return []

@st.cache_data(ttl=600, show_spinner=False)
def get_table_descriptions_with_ai(table_name: str):
    """AI機能を使ってテーブル・カラム説明を生成（10分キャッシュ）"""
    
    # まずAI_GENERATE_TABLE_DESCを試行
    try:
        # 複数の構文パターンを試行
        patterns_to_try = [
            f"SELECT SNOWFLAKE.CORTEX.AI_GENERATE_TABLE_DESC('{table_name}')",
            f'SELECT SNOWFLAKE.CORTEX.AI_GENERATE_TABLE_DESC("{table_name}")',
            f"SELECT SNOWFLAKE.CORTEX.AI_GENERATE_TABLE_DESC({table_name})"
        ]
        
        for pattern in patterns_to_try:
            try:
                ai_result = session.sql(pattern).collect()
                if ai_result and ai_result[0][0]:
                    import json
                    ai_data = json.loads(ai_result[0][0])
                    return {
                        'table_description': ai_data.get('table_description', ''),
                        'column_descriptions': ai_data.get('column_descriptions', {})
                    }
            except Exception:
                continue
                
    except Exception:
        pass
    
    # AI_GENERATE_TABLE_DESCが使えない場合、CORTEX.COMPLETEで代替実装
    try:
        # テーブル構造を取得
        quoted_table_name = f'"{table_name}"' if not table_name.startswith('"') else table_name
        describe_result = session.sql(f"DESCRIBE TABLE {quoted_table_name}").collect()
        
        if not describe_result:
            return None
            
        # カラム情報をまとめる
        columns_info = []
        for row in describe_result:
            columns_info.append(f"{row['name']} ({row['type']})")
        
        columns_text = "、".join(columns_info)  # 全カラムを対象
        
        # AI説明生成プロンプト（文字列エスケープ対応）
        table_desc_prompt = f"データベーステーブル {table_name} について分析してください。カラム構成: {columns_text}。このテーブルの用途と各カラムの意味を簡潔に説明してください。JSON形式で回答してください: {{\"table_description\": \"テーブルの用途説明\", \"column_descriptions\": {{\"カラム名\": \"説明\"}} }}"
        
        # CORTEX.COMPLETEで説明生成（プロンプトをエスケープ）
        escaped_prompt = table_desc_prompt.replace("'", "''")
        cortex_query = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('snowflake-arctic', '{escaped_prompt}')"
        cortex_result = session.sql(cortex_query).collect()
        
        if cortex_result and cortex_result[0][0]:
            import json
            import re
            
            # JSON部分を抽出
            response_text = cortex_result[0][0]
            json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
            
            if json_match:
                json_text = json_match.group(0)
                ai_data = json.loads(json_text)
                
                return {
                    'table_description': ai_data.get('table_description', ''),
                    'column_descriptions': ai_data.get('column_descriptions', {})
                }
                
    except Exception as e:
        # デバッグ用: エラー詳細を一時的に表示
        st.warning(f"AI説明生成でエラーが発生しました: {str(e)}")
        
    return None

@st.cache_data(ttl=600, show_spinner=False)
def get_table_columns_with_descriptions_cached(table_name: str):
    """テーブル/ビューのカラム名、データ型、AI生成説明を取得（10分キャッシュ）"""
    try:
        # 日本語テーブル名に対応するためダブルクォーテーションで囲む
        quoted_table_name = f'"{table_name}"' if not table_name.startswith('"') else table_name
        result = session.sql(f"DESCRIBE TABLE {quoted_table_name}").collect()
        columns_with_desc = []
        
        # まずAI_GENERATE_TABLE_DESCを試行
        ai_descriptions = get_table_descriptions_with_ai(table_name)
        
        for row in result:
            col_name = row['name']
            col_type = row['type']
            
            # 日本語カラム名に対応するためダブルクォーテーションで囲む
            quoted_col_name = f'"{col_name}"' if not col_name.startswith('"') else col_name
            
            # サンプル値を取得
            sample_text = ""
            try:
                # サンプルデータを取得（NULL以外の重複なし値を3件）
                sample_query = f"SELECT DISTINCT {quoted_col_name} FROM {quoted_table_name} WHERE {quoted_col_name} IS NOT NULL LIMIT 3"
                sample_result = session.sql(sample_query).collect()
                
                if sample_result:
                    sample_values = [str(row[0]) for row in sample_result]
                    sample_text = "、".join(sample_values[:3])  # 最大3件まで表示
                else:
                    sample_text = "（データなし）"
                    
            except Exception:
                sample_text = "（取得エラー）"
            
            # AI説明を取得（エラー時は空文字）
            ai_desc = ""
            if ai_descriptions and ai_descriptions.get('column_descriptions', {}).get(col_name):
                ai_desc = ai_descriptions['column_descriptions'][col_name]
            else:
                # AI説明が取得できない場合は空文字にして、サンプル値のみ表示
                ai_desc = ""
            
            columns_with_desc.append({
                'name': col_name, 
                'type': col_type, 
                'ai_description': ai_desc,
                'sample_values': sample_text
            })
        
        return columns_with_desc, ai_descriptions['table_description'] if ai_descriptions else None
    except Exception as e:
        st.error(f"テーブル情報取得エラー ({table_name}): {str(e)}")
        return [], None

def parse_relation_label(label: str) -> str:
    """[TABLE]/[VIEW] ラベルからオブジェクト名のみ取り出す"""
    return label.split(' ', 1)[1] if ' ' in label else label

def quote_identifier(identifier: str) -> str:
    """SQL識別子（テーブル名、カラム名）を適切にクォートする"""
    # 既にクォートされている場合はそのまま返す
    if identifier.startswith('"') and identifier.endswith('"'):
        return identifier
    
    # 以下の場合はクォートが必要：
    # 1. 日本語文字が含まれている
    # 2. 大文字・小文字が混在している
    # 3. 数字で始まる
    # 4. 特殊文字（スペース、ハイフンなど）が含まれている
    # 5. SQL予約語の場合
    import re
    
    # 日本語文字チェック
    has_japanese = re.search(r'[あ-んア-ンー一-龯]', identifier)
    
    # 特殊文字チェック（アンダースコア以外の非英数字）
    has_special_chars = re.search(r'[^\w]', identifier)
    
    # 大文字・小文字混在チェック
    has_mixed_case = identifier != identifier.upper() and identifier != identifier.lower()
    
    # 数字で始まるかチェック
    starts_with_digit = identifier[0].isdigit() if identifier else False
    
    # いずれかの条件に該当する場合はクォート
    if has_japanese or has_special_chars or has_mixed_case or starts_with_digit:
        return f'"{identifier}"'
    
    # 英数字・アンダースコアのみの小文字の場合はクォートしない
    return identifier

def is_date_type(data_type: str) -> bool:
    """データ型が日付型かどうかを判定する"""
    if not data_type:
        return False
    
    data_type_upper = data_type.upper()
    date_types = [
        'DATE', 'DATETIME', 'TIMESTAMP', 'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ', 'TIMESTAMP_TZ',
        'TIME', 'DATETIME_NTZ', 'DATETIME_LTZ', 'DATETIME_TZ'
    ]
    
    return any(date_type in data_type_upper for date_type in date_types)

def get_column_data_type(table_cols: list, column_name: str) -> str:
    """指定されたカラムのデータ型を取得する"""
    for col in table_cols:
        if col['name'] == column_name:
            return col['type']
    return ""


# =========================================================
# 実行ロジック
# =========================================================
def execute_query(search_query: str, all_rows: bool, limit_rows: int, show_sql: bool):
    """
    クエリを実行し、結果をセッション状態に保存する
    """
    def _sanitize_query(q: str) -> str:
        return q.strip().rstrip(';')
    
    def _fix_japanese_identifiers(query: str) -> str:
        """日本語のテーブル名・カラム名を自動的にクォートする（改良版）"""
        import re
        
        # 既にquote_identifierで生成されたクエリかチェック
        if '""' in query:
            # 二重クォートを修正
            query = query.replace('""', '"')
        
        # 日本語文字を含む識別子パターン（ただし既にクォートされていないもの）
        japanese_pattern = r'[あ-んア-ンー一-龯]+'
        
        # FROM句のテーブル名をクォート（既にクォートされていない場合のみ）
        def quote_table_name(match):
            full_match = match.group(0)
            table_name = match.group(1)
            if re.search(japanese_pattern, table_name) and not table_name.startswith('"'):
                return f'FROM "{table_name}"'
            return full_match
        
        query = re.sub(r'FROM\s+([^\s\'"]+)', quote_table_name, query, flags=re.IGNORECASE)
        
        # WHERE句のカラム名をクォート（値の部分は除外）
        def quote_where_column(match):
            full_match = match.group(0)
            col_name = match.group(1)
            if re.search(japanese_pattern, col_name) and not col_name.startswith('"'):
                return full_match.replace(col_name, f'"{col_name}"')
            return full_match
        
        # WHERE, AND, OR句での日本語カラム名をクォート（等号の前の部分のみ）
        query = re.sub(r'(WHERE|AND|OR)\s+([^\s\'"=<>!]+)\s*([=<>!]+)', quote_where_column, query, flags=re.IGNORECASE)
        
        return query

    try:
        base_query = _sanitize_query(search_query)
        
        # 保存時にquote_identifierで既に正しく処理されているため、
        # 実行時の自動修正は行わない（二重処理を避ける）
        final_query = base_query
        
        if (not all_rows) and " LIMIT " not in base_query.upper():
            final_query = f"{base_query} LIMIT {int(limit_rows)}"
        
        # SQL表示（show_sqlがTrueの場合）
        if show_sql:
            st.markdown("### 📝 実行SQL")
            st.code(final_query, language="sql")
            
            # 詳細情報も表示
            if base_query != final_query:
                with st.expander("🔍 SQL詳細情報", expanded=False):
                    st.write("**元のクエリ:**")
                    st.code(base_query, language="sql")
                    st.write("**LIMIT句追加後:**")
                    st.code(final_query, language="sql")

        with st.spinner("検索実行中..."):
            # まず件数チェック
            try:
                test_query = f"SELECT COUNT(*) FROM ({final_query})"
                row_count = session.sql(test_query).collect()[0][0]
                
                if row_count > 5000:
                    st.warning(f"検索結果が5,000行を超えています。表示に時間がかかる場合があります。取得件数: {row_count} 行")
                elif row_count == 0:
                    st.warning("検索条件に該当するデータがありません。")
                    
            except Exception as count_error:
                st.error(f"件数チェックエラー: {str(count_error)}")
                st.write("件数チェック用SQL:")
                st.code(test_query, language="sql")
                return

            # データ取得実行
            try:
                df_result = session.sql(final_query).to_pandas()
                st.session_state.last_result_df = df_result
                st.success(f"✅ 取得件数: {len(df_result)} 行。下部の『📄 出力結果』に表示しました。")
            except Exception as data_error:
                st.error(f"データ取得エラー: {str(data_error)}")
                st.write("データ取得用SQL:")
                st.code(final_query, language="sql")
                return

    except Exception as e:
        st.error(f"検索エラー: {str(e)}")
        st.write("実行クエリの参考:")
        try:
            st.code(final_query, language="sql")
        except:
            st.code(base_query, language="sql")

# =========================================================
# アプリケーション本体
# =========================================================

# タイトル
st.title("🔍 定型検索")
st.header("事前定義された検索テンプレートの管理と実行")

# ---
# 新規作成（メイン画面ワイドUI）
# ---
st.markdown("---")
st.subheader("➕ 新規検索オブジェクト作成")

colL, colR = st.columns([2, 3])
with colL:
    new_object_name = st.text_input("オブジェクト名", key="new_object_name", placeholder="例：口座を保有する東京都在住プレミア顧客の抽出")
    new_description = st.text_area("説明", key="new_description", placeholder="例：東京都在住のプレミアムランクの顧客データを抽出します。")
    
    relations = get_available_relations()
    selected_relation_label = st.selectbox("テーブル/ビューを選択", relations, key="new_relation_select")
    selected_table = parse_relation_label(selected_relation_label) if selected_relation_label else ""

      # 日付指定ブロック（独立・必須）
    st.markdown("#### 📅 日付指定（必須）")
    if selected_table:
        table_cols = get_table_columns_with_types_cached(selected_table)
        
        # 日付型カラムを抽出
        date_columns = [col for col in table_cols if is_date_type(col['type'])]
        
        if date_columns:
            st.info(f"📅 日付型カラムが {len(date_columns)} 件見つかりました")
            
            # 日付カラム選択
            date_col_options = [""] + [f"{col['name']} ({col['type']})" for col in date_columns]
            selected_date_col_label = st.selectbox(
                "日付カラムを選択",
                date_col_options,
                key="date_col_select",
                help="検索対象の日付カラムを選択してください"
            )
            
            if selected_date_col_label:
                # カラム名を抽出
                selected_date_col = selected_date_col_label.split(" (")[0]
                
                # 日付範囲指定
                col_date1, col_date2 = st.columns(2)
                with col_date1:
                    start_date = st.date_input(
                        "開始日",
                        value=datetime.now().date() - timedelta(days=30),  # デフォルト30日前
                        key="date_start"
                    )
                with col_date2:
                    end_date = st.date_input(
                        "終了日",
                        value=datetime.now().date(),
                        key="date_end"
                    )
                
                # 日付範囲の検証
                if start_date and end_date:
                    if start_date > end_date:
                        st.error("❌ 開始日は終了日より前の日付を指定してください")
                    else:
                        st.success(f"📅 検索期間: {start_date} 〜 {end_date} ({end_date - start_date + timedelta(days=1)}日間)")
                        
                        # 日付条件をセッション状態に保存
                        if 'date_condition' not in st.session_state:
                            st.session_state.date_condition = {}
                        
                        st.session_state.date_condition = {
                            "column": selected_date_col,
                            "start_date": start_date.strftime('%Y-%m-%d'),
                            "end_date": end_date.strftime('%Y-%m-%d')
                        }
        else:
            st.warning("⚠️ このテーブルには日付型カラムが見つかりませんでした")
            st.info("日付型カラムがない場合は、通常のフィルター条件を使用してください")
    else:
        st.info("テーブル/ビューを選択すると日付指定が可能になります")
        
    # WHERE句のGUI入力部分（日付以外の条件）
    st.markdown("#### フィルター条件 (WHERE句)")
    if selected_table:
        table_cols = get_table_columns_with_types_cached(selected_table)
        
        # 既存の条件の表示
        for i, condition in enumerate(st.session_state.where_conditions_list):
            op = "WHERE" if i == 0 else condition['logic_op']
            quoted_col = quote_identifier(condition['column'])
            st.write(f"**{op.upper()}** `{quoted_col}` {condition['operator']} `'{condition['value']}'`")
            if st.button("🗑️", key=f"del_cond_{i}"):
                del st.session_state.where_conditions_list[i]
                st.rerun()

        # 新しい条件の追加フォーム（日付以外）
        with st.expander("➕ 新しい条件を追加"):
            cond_logic_op = st.selectbox("論理演算子", ["AND", "OR"], key="cond_logic_op", disabled=(len(st.session_state.where_conditions_list) == 0))
            
            # 日付型以外のカラムのみを表示
            non_date_columns = [col for col in table_cols if not is_date_type(col['type'])]
            cond_col_name = st.selectbox("カラムを選択", [""] + sorted([c['name'] for c in non_date_columns]), key="cond_col_name")
            cond_operator = st.selectbox("演算子を選択", ["=", ">", "<", ">=", "<=", "<>", "LIKE"], key="cond_operator")
            cond_value = st.text_input("値を入力", key="cond_value")
            
            if st.button("追加", key="add_condition_btn") and cond_col_name and cond_value:
                st.session_state.where_conditions_list.append({
                    "logic_op": cond_logic_op,
                    "column": cond_col_name,
                    "operator": cond_operator,
                    "value": cond_value
                })
                st.success("条件を追加しました！")
                st.rerun()
                
                
    # ORDER BY句のGUI入力部分
    st.markdown("#### ソート条件 (ORDER BY句)")
    if selected_table:
        # 既存のソート条件の表示
        for i, condition in enumerate(st.session_state.order_by_conditions_list):
            quoted_col = quote_identifier(condition['column'])
            st.write(f"**ORDER BY** `{quoted_col}` **{condition['direction']}**")
            if st.button("🗑️", key=f"del_sort_{i}"):
                del st.session_state.order_by_conditions_list[i]
                st.rerun()

        # 新しいソート条件の追加フォーム
        with st.expander("➕ 新しいソート条件を追加"):
            sort_col_name = st.selectbox("ソート対象カラムを選択", [""] + sorted([c['name'] for c in table_cols]), key="sort_col_name")
            sort_direction = st.selectbox("ソート方向を選択", ["ASC", "DESC"], key="sort_direction", help="ASC: 昇順（小→大）、DESC: 降順（大→小）")
            
            if st.button("追加", key="add_sort_btn") and sort_col_name:
                st.session_state.order_by_conditions_list.append({
                    "column": sort_col_name,
                    "direction": sort_direction
                })
                st.success("ソート条件を追加しました！")
                st.rerun()
    else:
        st.info("テーブル/ビューを選択すると条件を設定できます。")

with colR:
    st.markdown("#### 出力項目 (SELECT句)")
    selected_columns = []
    if selected_table:
        # AI説明付きのカラム情報を取得するかどうかの選択
        use_ai_descriptions = st.toggle("🤖 AI生成テーブル・カラム説明を表示", value=True, 
                                      help="AI_GENERATE_TABLE_DESCを使ってテーブル全体の概要とカラム説明を自動生成します")
        
        if use_ai_descriptions:
            cols_with_info, table_description = get_table_columns_with_descriptions_cached(selected_table)
            
            # テーブル説明を表示
            if table_description:
                st.info(f"**📋 テーブル概要**: {table_description}")
        else:
            basic_cols = get_table_columns_with_types_cached(selected_table)
            cols_with_info = [{'name': c['name'], 'type': c['type'], 'ai_description': '', 'sample_values': ''} for c in basic_cols]
            table_description = None
        
        filter_text = st.text_input("カラム検索（部分一致）", key="col_filter_main")
        if filter_text:
            cols_with_info = [c for c in cols_with_info if filter_text.lower() in c['name'].lower()]
        
        c1, c2 = st.columns(2)
        with c1:
            if st.button("✅ 全選択", key="btn_select_all_cols_main"):
                st.session_state.new_selected_columns_state = set([c['name'] for c in cols_with_info])
                st.rerun()
        with c2:
            if st.button("🧹 全解除", key="btn_clear_cols_main"):
                st.session_state.new_selected_columns_state = set()
                st.rerun()

        display_data = []
        for c in cols_with_info:
            is_selected = c['name'] in st.session_state.new_selected_columns_state
            display_row = {
                '選択': is_selected,
                'カラム名': c['name'],
                'データ型': c['type']
            }
            if use_ai_descriptions:
                display_row['AI説明'] = c.get('ai_description', '')
                display_row['サンプル値'] = c.get('sample_values', '')
            display_data.append(display_row)
        
        df_cols = pd.DataFrame(display_data)

        if not df_cols.empty:
            column_config = {
                "選択": st.column_config.CheckboxColumn(
                    "選択",
                    help="表示するカラムを選択",
                    default=False
                ),
                "カラム名": st.column_config.TextColumn("カラム名", width="medium"),
                "データ型": st.column_config.TextColumn("データ型", width="small")
            }
            
            if use_ai_descriptions:
                column_config["AI説明"] = st.column_config.TextColumn("AI説明", width="large", help="AI_GENERATE_TABLE_DESCで生成された説明")
                column_config["サンプル値"] = st.column_config.TextColumn("サンプル値", width="medium", help="実際のデータサンプル")

            edited_df = st.data_editor(
                df_cols,
                column_config=column_config,
                hide_index=True,
                use_container_width=True,
                key="column_selection_editor"
            )

            selected_names = {row['カラム名'] for _, row in edited_df.iterrows() if row['選択']}
            st.session_state.new_selected_columns_state = selected_names
            
            selected_columns = sorted(list(st.session_state.new_selected_columns_state))
    else:
        st.info("テーブル/ビューを選択すると、カラム一覧が表示されます。")


# SQLプレビュー・保存
st.markdown("---")
colA, colB = st.columns([1, 2])
with colA:
    # 保存条件の判定
    has_date_condition = 'date_condition' in st.session_state and st.session_state.date_condition
    can_save = new_object_name and selected_table and has_date_condition
    
    if st.button("💾 保存", key="save_new_object_main", disabled=not can_save):
        # WHERE句の生成
        where_clauses = []
        
        # 日付条件を最初に追加（必須）
        if 'date_condition' in st.session_state and st.session_state.date_condition:
            date_cond = st.session_state.date_condition
            quoted_date_col = quote_identifier(date_cond['column'])
            date_clause = f"{quoted_date_col} BETWEEN '{date_cond['start_date']}' AND '{date_cond['end_date']}'"
            where_clauses.append(date_clause)
        
        # その他の条件を追加
        for i, cond in enumerate(st.session_state.where_conditions_list):
            quoted_col = quote_identifier(cond['column'])
            cond_str = f"{quoted_col} {cond['operator']}"
            if cond['operator'].upper() == 'LIKE':
                cond_str += f" '%{cond['value']}%'"
            else:
                cond_str += f" '{cond['value']}'"
            
            # 最初の条件以外は論理演算子を追加
            if where_clauses:  # 日付条件がある場合はANDを追加
                where_clauses.append(f"AND {cond_str}")
            else:
                where_clauses.append(cond_str)
        
        where_clause = " WHERE " + " ".join(where_clauses) if where_clauses else ""
        
        # ORDER BY句の生成
        order_by_clauses = []
        for cond in st.session_state.order_by_conditions_list:
            quoted_col = quote_identifier(cond['column'])
            order_by_clauses.append(f"{quoted_col} {cond['direction']}")
        
        order_by_clause = " ORDER BY " + ", ".join(order_by_clauses) if order_by_clauses else ""
        
        # SELECT句でカラム名をクォート
        if selected_columns:
            quoted_columns = [quote_identifier(col) for col in selected_columns]
            select_clause = ", ".join(quoted_columns)
        else:
            select_clause = "*"
        
        # テーブル名もクォート
        quoted_table = quote_identifier(selected_table)
        generated_query = f"SELECT {select_clause} FROM {quoted_table}{where_clause}{order_by_clause}"

        object_data = {
            'object_id': f"obj_{uuid.uuid4().hex[:12]}",
            'object_name': new_object_name,
            'description': new_description,
            'search_query': generated_query
        }
        if save_standard_search_object(object_data):
            st.success("検索オブジェクトを保存しました！")
            st.session_state.new_selected_columns_state = set()
            st.session_state.where_conditions_list = []
            st.session_state.order_by_conditions_list = []
            st.session_state.date_condition = {}
            st.rerun()
    if not can_save:
        if not new_object_name:
            st.warning("オブジェクト名を入力してください。")
        elif not selected_table:
            st.warning("テーブル/ビューを選択してください。")
        elif not has_date_condition:
            st.warning("📅 日付指定（必須）を設定してください。")

with colB:
    st.markdown("#### 📝 SQLプレビュー")
    if selected_table:
        # WHERE句の生成
        where_clauses = []
        
        # 日付条件を最初に追加（必須）
        if 'date_condition' in st.session_state and st.session_state.date_condition:
            date_cond = st.session_state.date_condition
            quoted_date_col = quote_identifier(date_cond['column'])
            date_clause = f"{quoted_date_col} BETWEEN '{date_cond['start_date']}' AND '{date_cond['end_date']}'"
            where_clauses.append(date_clause)
        
        # その他の条件を追加
        for i, cond in enumerate(st.session_state.where_conditions_list):
            quoted_col = quote_identifier(cond['column'])
            cond_str = f"{quoted_col} {cond['operator']}"
            if cond['operator'].upper() == 'LIKE':
                cond_str += f" '%{cond['value']}%'"
            else:
                cond_str += f" '{cond['value']}'"
            
            # 最初の条件以外は論理演算子を追加
            if where_clauses:  # 日付条件がある場合はANDを追加
                where_clauses.append(f"AND {cond_str}")
            else:
                where_clauses.append(cond_str)
        
        where_clause = " WHERE " + " ".join(where_clauses) if where_clauses else ""
        
        # ORDER BY句の生成
        order_by_clauses = []
        for cond in st.session_state.order_by_conditions_list:
            quoted_col = quote_identifier(cond['column'])
            order_by_clauses.append(f"{quoted_col} {cond['direction']}")
        
        order_by_clause = " ORDER BY " + ", ".join(order_by_clauses) if order_by_clauses else ""
        
        # SELECT句でカラム名をクォート
        if selected_columns:
            quoted_columns = [quote_identifier(col) for col in selected_columns]
            select_clause = ", ".join(quoted_columns)
        else:
            select_clause = "*"
        
        # テーブル名もクォート
        quoted_table = quote_identifier(selected_table)
        generated_query = f"SELECT {select_clause} FROM {quoted_table}{where_clause}{order_by_clause}"
        st.code(generated_query, language="sql")
        
        # ソート条件がある場合は追加情報を表示
        if order_by_clauses:
            st.info(f"📊 ソート条件: {len(order_by_clauses)}件設定済み")
    else:
        st.info("テーブル/ビューを選択するとSQLプレビューが表示されます。")

st.markdown("---")

# =========================================================
# タブ
# =========================================================
tab1, tab3 = st.tabs(["📋 オブジェクト一覧", "⭐ お気に入り"])
# tab2 = スケジュール実行タブ（機能不要のためコメントアウト）

with tab1:
    st.subheader("📋 定型検索オブジェクト一覧")
    # テーブルはsetup SQLで事前作成済み
    objects = load_standard_search_objects()
    if objects:
        for i, obj in enumerate(objects):
            with st.expander(f"🔍 {obj['OBJECT_NAME']} ({obj['OBJECT_ID']})", expanded=False):
                col1, col2 = st.columns([3, 2])
                with col1:
                    st.write(f"**説明**: {obj['DESCRIPTION'] or '説明なし'}")
                    # 作成日を日時（hh:mm）まで表示
                    created_at = obj['CREATED_AT']
                    if created_at:
                        if isinstance(created_at, str):
                            try:
                                from datetime import datetime
                                created_dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                                formatted_date = created_dt.strftime('%Y-%m-%d %H:%M')
                            except:
                                formatted_date = str(created_at)[:16]  # フォールバック
                        else:
                            formatted_date = created_at.strftime('%Y-%m-%d %H:%M')
                    else:
                        formatted_date = "不明"
                    st.write(f"**作成日**: {formatted_date}")
                    st.write(f"**実行回数**: {obj['EXECUTION_COUNT']}")
                    if obj['LAST_EXECUTED']:
                        last_executed = obj['LAST_EXECUTED']
                        if isinstance(last_executed, str):
                            try:
                                last_dt = datetime.fromisoformat(last_executed.replace('Z', '+00:00'))
                                formatted_last = last_dt.strftime('%Y-%m-%d %H:%M')
                            except:
                                formatted_last = str(last_executed)[:16]
                        else:
                            formatted_last = last_executed.strftime('%Y-%m-%d %H:%M')
                        st.write(f"**最終実行**: {formatted_last}")
                    
                    with col2:
                        all_rows = st.checkbox("全件取得 (LIMIT無効、非推奨)", value=False, key=f"allrows_{i}")
                        limit_rows = st.number_input("LIMIT行数", min_value=10, max_value=1000, value=100, step=50, key=f"limit_{i}", disabled=all_rows)
                        show_sql = st.checkbox("SQLを表示", value=False, key=f"show_sql_{i}")
                        
                        # SQLを表示（チェックボックスがONの場合、即座に表示）
                        if show_sql:
                            st.markdown("**📝 実行予定SQL:**")
                            # LIMIT句を考慮したSQLを生成
                            base_query = obj['SEARCH_QUERY']
                            if not all_rows and " LIMIT " not in base_query.upper():
                                display_query = f"{base_query} LIMIT {int(limit_rows)}"
                            else:
                                display_query = base_query
                            st.code(display_query, language="sql")
                        
                        # ボタンがクリックされたときの処理
                        if st.button("▶️ 実行", key=f"exec_btn_{i}"):
                            # 実行回数を更新
                            update_execution_count(obj['OBJECT_ID'])
                            
                            # 実行に必要な情報をセッション状態に保存し、再実行を要求
                            st.session_state.execute_query_request = {
                                "query": obj['SEARCH_QUERY'],
                                "all_rows": all_rows,
                                "limit_rows": limit_rows,
                                "show_sql": show_sql,
                            }
                            st.rerun()

                    fav_col = st.columns(1)[0]
                    with fav_col:
                        if 'favorites' not in st.session_state:
                            st.session_state.favorites = []
                        if obj['IS_FAVORITE']:
                            st.write("⭐ お気に入り済み")
                        else:
                            if st.button("⭐ お気に入り", key=f"favorite_{obj['OBJECT_ID']}_{i}"):
                                if add_to_favorites(obj['OBJECT_ID']):
                                    st.success("お気に入りに追加しました！")
                                    st.rerun()
        else:
            st.info("定型検索オブジェクトがありません。新規作成してください。")


with tab3:
    st.subheader("⭐ お気に入り")
    # テーブルはsetup SQLで事前作成済み
    favorite_objects = session.sql("SELECT * FROM application_db.application_schema.STANDARD_SEARCH_OBJECTS WHERE is_favorite = TRUE ORDER BY created_at DESC").collect()
    if favorite_objects:
            st.success(f"お気に入り: {len(favorite_objects)}件")
            for i, obj in enumerate(favorite_objects):
                with st.expander(f"⭐ {obj['OBJECT_NAME']} ({obj['OBJECT_ID']})", expanded=False):
                    col1, col2 = st.columns([3, 2])
                    with col1:
                        st.write(f"**説明**: {obj['DESCRIPTION'] or '説明なし'}")
                        # 作成日を日時（hh:mm）まで表示
                        created_at = obj['CREATED_AT']
                        if created_at:
                            if isinstance(created_at, str):
                                try:
                                    from datetime import datetime
                                    created_dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                                    formatted_date = created_dt.strftime('%Y-%m-%d %H:%M')
                                except:
                                    formatted_date = str(created_at)[:16]  # フォールバック
                            else:
                                formatted_date = created_at.strftime('%Y-%m-%d %H:%M')
                        else:
                            formatted_date = "不明"
                        st.write(f"**作成日**: {formatted_date}")
                        st.write(f"**実行回数**: {obj['EXECUTION_COUNT']}")
                        if obj['LAST_EXECUTED']:
                            last_executed = obj['LAST_EXECUTED']
                            if isinstance(last_executed, str):
                                try:
                                    last_dt = datetime.fromisoformat(last_executed.replace('Z', '+00:00'))
                                    formatted_last = last_dt.strftime('%Y-%m-%d %H:%M')
                                except:
                                    formatted_last = str(last_executed)[:16]
                            else:
                                formatted_last = last_executed.strftime('%Y-%m-%d %H:%M')
                            st.write(f"**最終実行**: {formatted_last}")
                    with col2:
                        all_rows = st.checkbox("全件取得 (LIMIT無効、非推奨)", value=False, key=f"fav_allrows_{i}")
                        limit_rows = st.number_input("LIMIT行数", min_value=10, max_value=5000, value=5000, step=10, key=f"fav_limit_{i}", disabled=all_rows)
                        show_sql = st.checkbox("SQLを表示", value=False, key=f"fav_show_sql_{i}")

                        # SQLを表示（チェックボックスがONの場合、即座に表示）
                        if show_sql:
                            st.markdown("**📝 実行予定SQL:**")
                            # LIMIT句を考慮したSQLを生成
                            base_query = obj['SEARCH_QUERY']
                            if not all_rows and " LIMIT " not in base_query.upper():
                                display_query = f"{base_query} LIMIT {int(limit_rows)}"
                            else:
                                display_query = base_query
                            st.code(display_query, language="sql")

                        if st.button("▶️ 実行", key=f"fav_exec_btn_{i}"):
                            # 実行回数を更新
                            update_execution_count(obj['OBJECT_ID'])
                            
                            st.session_state.execute_query_request = {
                                "query": obj['SEARCH_QUERY'],
                                "all_rows": all_rows,
                                "limit_rows": limit_rows,
                                "show_sql": show_sql,
                            }
                            st.rerun()
    else:
        st.info("お気に入りの検索オブジェクトがありません。")
        st.info("検索オブジェクト一覧から⭐ボタンをクリックしてお気に入りに追加してください。")

# =========================================================
# セッション状態のクエリ実行リクエストを処理
# =========================================================
if st.session_state.execute_query_request is not None:
    request = st.session_state.execute_query_request
    execute_query(
        search_query=request["query"],
        all_rows=request["all_rows"],
        limit_rows=request["limit_rows"],
        show_sql=request["show_sql"]
    )
    # リクエストを初期化してループを防ぐ
    st.session_state.execute_query_request = None

# =========================================================
# 大きな帳票形式の出力結果ビューア
# =========================================================
st.markdown("---")
st.subheader("📄 出力結果")
if st.session_state.last_result_df is not None:
    st.dataframe(st.session_state.last_result_df, use_container_width=True, height=600)
    csv = st.session_state.last_result_df.to_csv(index=False)
    st.download_button(label="💾 CSVダウンロード", data=csv, file_name=f"result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", mime="text/csv")
else:
    st.info("ここに最新の実行結果を表示します。上部で検索を実行してください。")

st.markdown("---")
st.markdown("**📊 Streamlitデータアプリ | 定型検索 - ©Snowflake合同会社**")
