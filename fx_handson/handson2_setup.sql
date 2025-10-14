USE ROLE accountadmin;
USE WAREHOUSE compute_wh;

use database fx_handson;
use schema fx_handson_schema;

-- Cortex Analystを作成するためのセマンティックビュー作成用のストアドプロシージャ
CREATE OR REPLACE PROCEDURE GENERATE_SEMANTIC_VIEW(
    SOURCE_DATABASE VARCHAR,      -- ソースデータベース名
    SOURCE_SCHEMA VARCHAR,        -- ソーススキーマ名
    SOURCE_TABLE VARCHAR,         -- ソーステーブル名
    TARGET_DATABASE VARCHAR,      -- ターゲットデータベース名
    TARGET_SCHEMA VARCHAR,        -- ターゲットスキーマ名
    TARGET_VIEW_NAME VARCHAR,     -- 作成するセマンティックビュー名
    LLM_MODEL VARCHAR DEFAULT 'openai-gpt-4.1' -- 使用するLLMモデル
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    -- テーブル情報
    column_info VARCHAR;
    sample_data_json VARCHAR;
    table_comment VARCHAR;
    
    -- AI生成結果
    ai_response VARCHAR;
    
    -- 最終SQL
    create_view_sql VARCHAR;
    
BEGIN
    -- ================================================================================
    -- STEP 1: テーブル情報の取得
    -- ================================================================================
    
    -- カラム情報を取得 （DESCRIBE TABLEを使用）
    DECLARE
        describe_sql VARCHAR;
        rs RESULTSET;
    BEGIN
        describe_sql := 'DESCRIBE TABLE ' || :SOURCE_DATABASE || '.' || :SOURCE_SCHEMA || '.' || :SOURCE_TABLE;
        rs := (EXECUTE IMMEDIATE :describe_sql);
        
        -- 結果から情報を取得
        -- $1: カラム名, $2: データ型, $10: コメント
        SELECT LISTAGG(
            $1 || ':' || $2 || CASE WHEN $10 IS NOT NULL AND $10 != '' THEN '(' || $10 || ')' ELSE '' END,
            ', '
        ) INTO column_info
        FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
        
    EXCEPTION
        WHEN OTHER THEN
            RETURN 'エラー: テーブル情報を取得できません - ' || :SOURCE_DATABASE || '.' || :SOURCE_SCHEMA || '.' || :SOURCE_TABLE || '. エラー: ' || SQLERRM;
    END;
    
    -- エラーチェック
    IF (column_info IS NULL OR LENGTH(:column_info) = 0) THEN
        RETURN 'エラー: カラム情報が空です - ' || :SOURCE_DATABASE || '.' || :SOURCE_SCHEMA || '.' || :SOURCE_TABLE;
    END IF;
    
    -- テーブルコメントを取得
    SELECT COMMENT INTO table_comment
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_CATALOG = :SOURCE_DATABASE
      AND TABLE_SCHEMA = :SOURCE_SCHEMA
      AND TABLE_NAME = :SOURCE_TABLE;
    
    -- サンプルデータを取得 （先頭5行）
    DECLARE
        sample_sql VARCHAR;
        sample_rs RESULTSET;
    BEGIN
        sample_sql := 'SELECT * FROM ' || :SOURCE_DATABASE || '.' || :SOURCE_SCHEMA || '.' || :SOURCE_TABLE || ' LIMIT 5';
        sample_rs := (EXECUTE IMMEDIATE :sample_sql);
        
        -- サンプルデータをJSON形式で取得
        SELECT TO_VARCHAR(ARRAY_AGG(OBJECT_CONSTRUCT(*))) INTO sample_data_json
        FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
        
    EXCEPTION
        WHEN OTHER THEN
            sample_data_json := 'サンプルデータ取得不可';
    END;
    
    -- ================================================================================
    -- STEP 2: AIで完全なSemantic View定義を生成
    -- ================================================================================
    
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
        :LLM_MODEL,
        CONCAT(
            '【重要】以下の情報を使用してSnowflakeのセマンティックビューを生成してください。',
            '\n\n★存在しないカラム名は絶対に使わないでください★',
            '\n\n【テーブル情報】',
            '\nテーブル: ', :SOURCE_DATABASE, '.', :SOURCE_SCHEMA, '.', :SOURCE_TABLE,
            '\nエイリアス: ', :SOURCE_TABLE,
            '\nテーブルコメント: ', COALESCE(:table_comment, 'なし'),
            '\n\n【実際に存在するカラム (これ以外は使用禁止)】',
            '\n', :column_info,
            '\n\n【サンプルデータ (内容を理解するための参考情報)】',
            '\n', SUBSTRING(:sample_data_json, 1, 1000),
            '\n\n【生成するSQL】',
            '\nCREATE OR REPLACE SEMANTIC VIEW ', :TARGET_DATABASE, '.', :TARGET_SCHEMA, '.', :TARGET_VIEW_NAME,
            '\n  TABLES (',
            '\n    ', :SOURCE_TABLE, ' AS ', :SOURCE_DATABASE, '.', :SOURCE_SCHEMA, '.', :SOURCE_TABLE,
            '\n      WITH SYNONYMS (''日本語別名1'', ''日本語別名2'', ''日本語別名3'', ''日本語別名4'')',
            '\n      COMMENT = ''テーブルの詳細な日本語説明''',
            '\n  )',
            '\n  FACTS (',
            '\n    ', :SOURCE_TABLE, '.fact_name AS 実在するカラム名',
            '\n      WITH SYNONYMS (''別名1'', ''別名2'')',
            '\n      COMMENT = ''カラムの詳細な日本語の説明''',
            '\n  )',
            '\n  DIMENSIONS (',
            '\n    ', :SOURCE_TABLE, '.dim_name AS 実在するカラム名',
            '\n      WITH SYNONYMS (''別名1'', ''別名2'')',
            '\n      COMMENT = ''カラムの詳細な日本語の説明''',
            '\n  )',
            '\n  METRICS (',
            '\n    ', :SOURCE_TABLE, '.metric_name AS SUM(実在するカラム名)',
            '\n      WITH SYNONYMS (''別名1'', ''別名2'')',
            '\n      COMMENT = ''カラムの詳細な日本語の説明''',
            '\n  )',
            '\n  COMMENT = ''セマンティックビューの詳細な日本語の説明'';',
            '\n\n【絶対に守るルール】',
            '\n★★★最重要★★★ 上記カラムリストに存在するカラム名のみ使用',
            '\n\n0. Primary Keyの省略：',
            '\n   - PRIMARY KEY句は記述しないでください',
            '\n   - Snowflakeでは主キー制約は機能せず必須ではないため省略',
            '\n\n1. カラム名の使用：',
            '\n   - ASの右側で使うカラム名も、上記リストに存在するものだけ',
            '\n   - 例：YEAR(TRANSACTION_DATE) ← TRANSACTION_DATEが実在する場合のみOK',
            '\n   - 例：YEAR(ORDER_DATE) ← ORDER_DATEが存在しない場合は絶対NG',
            '\n\n2. FACTS (数値データ)：',
            '\n   - 数量、金額、価格、スコア、率などの数値型カラム',
            '\n   - 単純なカラム参照と計算式の両方を含める',
            '\n   - 例 (単純)：QUANTITY、PRICE、AMOUNT',
            '\n   - 例 (計算)：revenue AS テーブル名.QUANTITY * テーブル名.UNIT_PRICE のように「新しいファクト名 AS 計算式」の形式で記述',
            '\n   - 例 (計算)：revenue AS QUANTITY * UNIT_PRICE、discount_amount AS TOTAL_PRICE * 0.1',
            '\n\n3. DIMENSIONS (属性データ)：',
            '\n   - ID、名前、日付、カテゴリ、ステータス',
            '\n   - FACTSと重複しない',
            '\n\n4. 名前の重複禁止：左側のテーブル.名前は全て異なる',
            '\n\n5. 日付派生の命名：元カラム名_year、元カラム名_month',
            '\n\n6. シノニムの充実化 (Cortex Analystがユーザーの依頼を正確に理解するために重要)：',
            '\n   - テーブル：4-5個の業務で使われる別名',
            '\n   - 各DIMENSIONS、FACTS、METRICS：2-3個の自然な別名',
            '\n   - サンプルデータから実際の用途を推測して適切な別名を選択',
            '\n   - 例：売上金額 → 「総売上」「売上合計」「販売額」',
            '\n   - 例：商品ID → 「プロダクトID」「商品番号」「品番」',
            '\n\n7. COMMENTの充実化：',
            '\n   - サンプルデータから具体的な説明を生成',
            '\n   - ビジネス的な意味を含める',
            '\n   - 単位や範囲も記載',
            '\n\n8. サンプルデータの活用：',
            '\n   - サンプルデータの内容から実際の用途を理解',
            '\n   - より具体的で実用的なシノニムを生成',
            '\n   - データの特性（範囲、単位等）をCOMMENTに反映',
            '\n\n9. 豊富なFACTSとDIMENSIONSを生成：',
            '\n   - FACTSは5-10個程度 (単純なカラム + 計算式)',
            '\n   - DIMENSIONSは5-15個程度 (元カラム + 日付派生)',
            '\n   - METRICSは4-8個程度 (様々な集計パターン)',
            '\n\n10. 出力：SQL構文のみ (```、説明文は不要)',
            '\n\n11. 日本語：シノニムとCOMMENTは全て日本語',
            '\n\nCREATE SEMANTIC VIEW文のみを出力:'
        )
    ) INTO ai_response;
    
    -- デバッグ： AI応答の長さを確認
    IF (:ai_response IS NULL OR LENGTH(:ai_response) = 0) THEN
        RETURN 'エラー: AI_COMPLETEが空の応答を返しました。カラム情報: ' || SUBSTRING(:column_info, 1, 500);
    END IF;
    
    -- 余計な文字を削除
    create_view_sql := :ai_response;
    create_view_sql := REPLACE(:create_view_sql, '```sql', '');
    create_view_sql := REPLACE(:create_view_sql, '```', '');
    create_view_sql := REPLACE(:create_view_sql, '"', '''');
    create_view_sql := REPLACE(:create_view_sql, '修正後：', '');
    create_view_sql := REPLACE(:create_view_sql, '修正内容：', '');
    create_view_sql := REPLACE(:create_view_sql, '**', '');
    create_view_sql := TRIM(:create_view_sql);
    
    -- 説明文を削除 （これにより～などで始まる行を削除）
    IF (:create_view_sql LIKE '%これにより%') THEN
        create_view_sql := SPLIT_PART(:create_view_sql, 'これにより', 1);
    END IF;
    IF (:create_view_sql LIKE '%→%') THEN
        create_view_sql := SPLIT_PART(:create_view_sql, '→', 1);
    END IF;
    
    create_view_sql := TRIM(:create_view_sql);
    
    -- 最終確認： SQL文が空でないかチェック
    IF (:create_view_sql IS NULL OR LENGTH(:create_view_sql) < 50) THEN
        RETURN 'エラー: 生成されたSQLが空または短すぎます。AI応答: ' || SUBSTRING(:ai_response, 1, 1000);
    END IF;
    
    -- ================================================================================
    -- STEP 4: セマンティックビューを作成
    -- ================================================================================
    
    BEGIN
        EXECUTE IMMEDIATE :create_view_sql;
        RETURN 'セマンティックビュー ' || :TARGET_DATABASE || '.' || :TARGET_SCHEMA || '.' || :TARGET_VIEW_NAME || ' が正常に作成されました。SQL: ' || :create_view_sql;
    EXCEPTION
        WHEN OTHER THEN
            RETURN 'エラー: ' || SQLERRM || ' -- 生成されたSQL: ' || SUBSTRING(:create_view_sql, 1, 2000);
    END;
    
END;
$$;

-- セマンティックビュー作成用の為替データに移動平均を追加したテーブルを作成
CREATE OR REPLACE TABLE FX_ALL_ANALYSIS AS
SELECT
    DATE,
    VARIABLE_NAME,
    BASE_CURRENCY_ID,
    QUOTE_CURRENCY_ID,
    VALUE AS EXCHANGE_RATE,
    
    -- 移動平均の計算
    AVG(VALUE) OVER (
        ORDER BY DATE 
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) AS MA_5,
    
    AVG(VALUE) OVER (
        ORDER BY DATE 
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS MA_20,
    
    AVG(VALUE) OVER (
        ORDER BY DATE 
        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
    ) AS MA_50,
    
    -- 日次リターン
    -- (VALUE - LAG(VALUE, 1) OVER (ORDER BY DATE)) / LAG(VALUE, 1) OVER (ORDER BY DATE) * 100 AS DAILY_RETURN
    
FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.FX_RATES_TIMESERIES
ORDER BY DATE DESC;

call generate_semantic_view(
    'FX_HANDSON',
    'FX_HANDSON_SCHEMA',
    'FX_ALL_ANALYSIS',
    'FX_HANDSON',
    'FX_HANDSON_SCHEMA',
    'FX_ALL_SEMANTIC_VIEW',
    'openai-gpt-4.1'
);

