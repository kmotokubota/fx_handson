"""
楽天銀行向け 為替分析システム
Multi-Currency FX Analytics Dashboard

機能:
- 単一通貨ペアの詳細分析
- 複数通貨ペアの比較分析
- テクニカル指標（RSI, MACD, ボリンジャーバンド等）
- AI市場分析（Snowflake Cortex）
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from snowflake.snowpark.context import get_active_session
from datetime import datetime, timedelta, date
import re
import warnings
warnings.filterwarnings('ignore')

# AI_COMPLETE関数用のLLMモデル選択肢
AI_COMPLETE_MODELS = [
    "llama4-maverick",
    "openai-gpt-4.1",
    "claude-4.5-sonnet", 
    "mistral-large2"
]

# ページ設定
st.set_page_config(
    page_title="為替分析システム",
    page_icon="💱",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Snowflakeセッション取得
@st.cache_resource
def get_snowflake_session():
    return get_active_session()

session = get_snowflake_session()

# カスタムCSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #5a6c7d;
        text-align: center;
        margin-bottom: 1rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .analysis-section {
        background-color: #ffffff;
        padding: 1.5rem;
        border-radius: 0.5rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin-bottom: 1rem;
    }
    .ai-insight {
        background-color: #e8f4f8;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #17a2b8;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# 利用可能な通貨ペア取得関数
@st.cache_data(ttl=86400)  # 24時間キャッシュ
def get_available_currency_pairs():
    """利用可能な通貨ペアを取得"""
    # まずテーブルの存在とカラムを確認
    try:
        # シンプルなクエリでテーブルの存在を確認
        test_query = """
        SELECT *
        FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.FX_RATES_TIMESERIES
        LIMIT 1
        """
        test_df = session.sql(test_query).to_pandas()
        
        # カラムの存在を確認
        has_currency_names = 'BASE_CURRENCY_NAME' in test_df.columns and 'QUOTE_CURRENCY_NAME' in test_df.columns
        
        # クエリを構築
        if has_currency_names:
            query = """
            SELECT DISTINCT 
                BASE_CURRENCY_ID,
                QUOTE_CURRENCY_ID,
                BASE_CURRENCY_NAME,
                QUOTE_CURRENCY_NAME,
                VARIABLE_NAME
            FROM
                SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.FX_RATES_TIMESERIES
            WHERE
                DATE >= CURRENT_DATE - 365
            ORDER BY
                BASE_CURRENCY_ID, QUOTE_CURRENCY_ID
            """
        else:
            # カラムがない場合は基本情報のみ取得
            query = """
            SELECT DISTINCT 
                BASE_CURRENCY_ID,
                QUOTE_CURRENCY_ID,
                VARIABLE_NAME
            FROM
                SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.FX_RATES_TIMESERIES
            WHERE
                DATE >= CURRENT_DATE - 365
            ORDER BY
                BASE_CURRENCY_ID, QUOTE_CURRENCY_ID
            """
        
        df = session.sql(query).to_pandas()
        
        # データが取得できた場合のみ処理
        if not df.empty:
            # 通貨ペアの表示名を作成
            df['PAIR_DISPLAY'] = df['BASE_CURRENCY_ID'] + '/' + df['QUOTE_CURRENCY_ID']
            
            # 通貨名のカラムが存在する場合
            if 'BASE_CURRENCY_NAME' in df.columns and 'QUOTE_CURRENCY_NAME' in df.columns:
                df['PAIR_FULL_NAME'] = df['BASE_CURRENCY_NAME'] + ' / ' + df['QUOTE_CURRENCY_NAME']
            else:
                # 通貨名がない場合はIDをそのまま使用
                df['PAIR_FULL_NAME'] = df['PAIR_DISPLAY']
                # デフォルトの通貨名を追加
                df['BASE_CURRENCY_NAME'] = df['BASE_CURRENCY_ID']
                df['QUOTE_CURRENCY_NAME'] = df['QUOTE_CURRENCY_ID']
        else:
            # データが空の場合、デフォルトの通貨ペアを作成
            df = pd.DataFrame({
                'BASE_CURRENCY_ID': ['USD', 'EUR', 'GBP'],
                'QUOTE_CURRENCY_ID': ['JPY', 'JPY', 'JPY'],
                'BASE_CURRENCY_NAME': ['US Dollar', 'Euro', 'British Pound'],
                'QUOTE_CURRENCY_NAME': ['Japanese Yen', 'Japanese Yen', 'Japanese Yen'],
                'VARIABLE_NAME': ['USD/JPY Exchange Rate', 'EUR/JPY Exchange Rate', 'GBP/JPY Exchange Rate'],
                'PAIR_DISPLAY': ['USD/JPY', 'EUR/JPY', 'GBP/JPY'],
                'PAIR_FULL_NAME': ['US Dollar / Japanese Yen', 'Euro / Japanese Yen', 'British Pound / Japanese Yen']
            })
        
        return df
    
    except Exception as e:
        # エラー時のデフォルトデータ
        st.warning(f"⚠️ データ取得エラー: {str(e)}. デフォルトの通貨ペアを使用します。")
        return pd.DataFrame({
            'BASE_CURRENCY_ID': ['USD', 'EUR', 'GBP'],
            'QUOTE_CURRENCY_ID': ['JPY', 'JPY', 'JPY'],
            'BASE_CURRENCY_NAME': ['US Dollar', 'Euro', 'British Pound'],
            'QUOTE_CURRENCY_NAME': ['Japanese Yen', 'Japanese Yen', 'Japanese Yen'],
            'VARIABLE_NAME': ['USD/JPY Exchange Rate', 'EUR/JPY Exchange Rate', 'GBP/JPY Exchange Rate'],
            'PAIR_DISPLAY': ['USD/JPY', 'EUR/JPY', 'GBP/JPY'],
            'PAIR_FULL_NAME': ['US Dollar / Japanese Yen', 'Euro / Japanese Yen', 'British Pound / Japanese Yen']
        })

# データ取得関数
@st.cache_data(ttl=3600)
def load_fx_data(start_date, end_date, base_currency='USD', quote_currency='JPY'):
    """指定通貨ペアの為替データを取得"""
    query = f"""
    SELECT
        DATE,
        VALUE AS EXCHANGE_RATE,
        VARIABLE_NAME,
        BASE_CURRENCY_ID,
        QUOTE_CURRENCY_ID
    FROM
        SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.FX_RATES_TIMESERIES
    WHERE
        BASE_CURRENCY_ID = '{base_currency}'
        AND QUOTE_CURRENCY_ID = '{quote_currency}'
        AND DATE >= '{start_date}'
        AND DATE <= '{end_date}'
    ORDER BY
        DATE
    """
    
    df = session.sql(query).to_pandas()
    if not df.empty:
        df['DATE'] = pd.to_datetime(df['DATE'])
        df = df.sort_values('DATE')
    return df

# 複数通貨ペア対応データ取得関数
@st.cache_data(ttl=3600)
def load_multiple_fx_data(start_date, end_date, currency_pairs):
    """複数通貨ペアの為替データを取得"""
    all_data = {}
    
    for pair in currency_pairs:
        base_currency, quote_currency = pair.split('/')
        df = load_fx_data(start_date, end_date, base_currency, quote_currency)
        if not df.empty:
            pair_name = f"{base_currency}/{quote_currency}"
            all_data[pair_name] = df
            
    return all_data

# テクニカル指標計算関数
def calculate_technical_indicators(df, price_col='EXCHANGE_RATE'):
    """テクニカル指標を計算"""
    df = df.copy()
    
    # 移動平均
    df['MA_5'] = df[price_col].rolling(window=5).mean()
    df['MA_20'] = df[price_col].rolling(window=20).mean()
    df['MA_50'] = df[price_col].rolling(window=50).mean()
    df['MA_200'] = df[price_col].rolling(window=200).mean()
    
    # ボリンジャーバンド
    df['BB_Middle'] = df[price_col].rolling(window=20).mean()
    bb_std = df[price_col].rolling(window=20).std()
    df['BB_Upper'] = df['BB_Middle'] + (bb_std * 2)
    df['BB_Lower'] = df['BB_Middle'] - (bb_std * 2)
    
    # RSI
    delta = df[price_col].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['RSI'] = 100 - (100 / (1 + rs))
    
    # MACD
    exp1 = df[price_col].ewm(span=12, adjust=False).mean()
    exp2 = df[price_col].ewm(span=26, adjust=False).mean()
    df['MACD'] = exp1 - exp2
    df['MACD_Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
    df['MACD_Histogram'] = df['MACD'] - df['MACD_Signal']
    
    # ストキャスティクス
    high_14 = df[price_col].rolling(window=14).max()
    low_14 = df[price_col].rolling(window=14).min()
    df['Stoch_K'] = 100 * ((df[price_col] - low_14) / (high_14 - low_14))
    df['Stoch_D'] = df['Stoch_K'].rolling(window=3).mean()
    
    # ADX (Average Directional Index)
    price_diff = df[price_col].diff()
    df['TR'] = df[price_col].diff().abs()
    df['ATR'] = df['TR'].rolling(window=14).mean()
    df['DM_Plus'] = price_diff.where(price_diff > 0, 0)
    df['DM_Minus'] = (-price_diff).where(price_diff < 0, 0)
    df['DI_Plus'] = 100 * (df['DM_Plus'].rolling(window=14).mean() / df['ATR'])
    df['DI_Minus'] = 100 * (df['DM_Minus'].rolling(window=14).mean() / df['ATR'])
    df['DX'] = 100 * abs(df['DI_Plus'] - df['DI_Minus']) / (df['DI_Plus'] + df['DI_Minus'])
    df['ADX'] = df['DX'].rolling(window=14).mean()
    
    # ボラティリティ
    df['Volatility'] = df[price_col].rolling(window=20).std()
    
    # 日次リターン
    df['Daily_Return'] = df[price_col].pct_change()
    
    return df

# AI分析関数
def get_ai_analysis(df, analysis_type, currency_pair="USD/JPY", model="llama4-maverick"):
    """AI_COMPLETE関数を使用した分析"""
    
    # 最新データの準備
    latest_rate = df['EXCHANGE_RATE'].iloc[-1]
    prev_rate = df['EXCHANGE_RATE'].iloc[-2] if len(df) > 1 else latest_rate
    change = latest_rate - prev_rate
    change_pct = (change / prev_rate) * 100 if prev_rate != 0 else 0
    
    # 基本統計
    min_rate = df['EXCHANGE_RATE'].min()
    max_rate = df['EXCHANGE_RATE'].max()
    avg_rate = df['EXCHANGE_RATE'].mean()
    volatility = df['EXCHANGE_RATE'].std()
    
    # 最近のトレンド
    recent_data = df.tail(10)
    recent_trend = "上昇" if recent_data['EXCHANGE_RATE'].iloc[-1] > recent_data['EXCHANGE_RATE'].iloc[0] else "下降"
    
    if analysis_type == "market_trend":
        prompt = f"""
        {currency_pair}為替レートの市場分析をお願いします。
        
        現在のレート: {latest_rate:.4f}
        前日比: {change:+.4f} ({change_pct:+.2f}%)
        期間内最高値: {max_rate:.4f}
        期間内最安値: {min_rate:.4f}
        平均レート: {avg_rate:.4f}
        ボラティリティ: {volatility:.2f}
        最近のトレンド: {recent_trend}傾向
        
        プロのエコノミストとして、以下の観点から分析してください：
        1. 現在の市場状況の評価
        2. トレンドの要因分析
        3. 今後の見通し
        4. リスク要因
        """
        
    elif analysis_type == "technical_analysis":
        rsi = df['RSI'].iloc[-1] if 'RSI' in df.columns and pd.notna(df['RSI'].iloc[-1]) else None
        macd = df['MACD'].iloc[-1] if 'MACD' in df.columns and pd.notna(df['MACD'].iloc[-1]) else None
        
        prompt = f"""
        {currency_pair}為替レートのテクニカル分析をお願いします。
        
        現在のレート: {latest_rate:.4f}
        RSI: {rsi:.1f if rsi else 'N/A'}
        MACD: {macd:.4f if macd else 'N/A'}
        ボラティリティ: {volatility:.4f}
        
        テクニカルアナリストとして、以下を分析してください：
        1. チャートパターンの評価
        2. 売買シグナルの状況
        3. サポート・レジスタンスレベル
        4. 短期的な方向性
        """
        
    elif analysis_type == "risk_assessment":
        prompt = f"""
        {currency_pair}為替レートのリスク評価をお願いします。
        
        現在のボラティリティ: {volatility:.4f}
        最近の最大変動幅: {max_rate - min_rate:.4f}
        日次変動率の標準偏差: {df['Daily_Return'].std()*100:.2f}%
        
        リスク管理の専門家として、以下を評価してください：
        1. 現在のボラティリティレベル
        2. 主要なリスク要因
        3. ヘッジ戦略の提案
        4. 注意すべき経済指標
        """
    
    try:
        # AI_COMPLETE関数の実行
        # プロンプトのエスケープ処理
        escaped_prompt = prompt.replace("'", "''")
        
        ai_query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            '{model}',
            '{escaped_prompt}'
        ) as analysis
        """
        
        result = session.sql(ai_query).collect()
        return result[0]['ANALYSIS'] if result else "AI分析を取得できませんでした。"
        
    except Exception as e:
        return f"AI分析でエラーが発生しました: {str(e)}"

# メインアプリケーション
def main():
    # ヘッダー
    st.markdown('<div class="main-header">💱 為替分析システム</div>', unsafe_allow_html=True)
    
    # 利用可能な通貨ペアを取得
    try:
        with st.spinner("利用可能な通貨ペアを取得中..."):
            currency_pairs_df = get_available_currency_pairs()
            
        # データが空でないか確認
        if currency_pairs_df.empty:
            st.error("❌ 通貨ペアデータが取得できませんでした。データベースの接続を確認してください。")
            return
                
    except Exception as e:
        st.error(f"❌ 通貨ペア情報の取得に失敗しました: {str(e)}")
        st.info("💡 データベース接続を確認するか、Streamlitアプリを再起動してください。")
        
        # キャッシュクリアボタン
        if st.button("🔄 キャッシュをクリアして再試行"):
            st.cache_data.clear()
            st.rerun()
        return
    
    # サイドバー設定
    with st.sidebar:
        st.header("📊 分析設定")
        
        # 分析モード選択
        st.subheader("🎯 分析モード")
        analysis_mode = st.radio(
            "モードを選択",
            ["単一通貨ペア分析", "複数通貨ペア比較"],
            help="単一通貨ペアは詳細なテクニカル分析、複数通貨ペアは比較分析が可能です"
        )
        
        st.markdown("---")
        
        # 通貨ペア選択
        st.subheader("💱 通貨ペア選択")
        
        if analysis_mode == "単一通貨ペア分析":
            # 単一通貨ペア選択 - BASE と QUOTE を別々に選択
            # 利用可能なBASE_CURRENCYとQUOTE_CURRENCYのリストを取得
            available_base_currencies = sorted(currency_pairs_df['BASE_CURRENCY_ID'].unique().tolist())
            
            # BASE_CURRENCY選択
            base_currency = st.selectbox(
                "🔵 基軸通貨 (Base Currency)",
                available_base_currencies,
                index=available_base_currencies.index('USD') if 'USD' in available_base_currencies else 0,
                help="為替レートの基準となる通貨を選択"
            )
            
            # 選択されたBASE_CURRENCYで利用可能なQUOTE_CURRENCYをフィルタリング
            available_quote_currencies = sorted(
                currency_pairs_df[currency_pairs_df['BASE_CURRENCY_ID'] == base_currency]['QUOTE_CURRENCY_ID'].unique().tolist()
            )
            
            # QUOTE_CURRENCY選択
            quote_currency = st.selectbox(
                "🟢 決済通貨 (Quote Currency)",
                available_quote_currencies,
                index=available_quote_currencies.index('JPY') if 'JPY' in available_quote_currencies else 0,
                help="為替レートで表示される通貨を選択"
            )
            
            # 選択された通貨ペアの情報を表示
            selected_pairs = [f"{base_currency}/{quote_currency}"]
            pair_info = currency_pairs_df[
                (currency_pairs_df['BASE_CURRENCY_ID'] == base_currency) & 
                (currency_pairs_df['QUOTE_CURRENCY_ID'] == quote_currency)
            ]
            
            if not pair_info.empty:
                st.success(f"✅ 選択中: **{base_currency}/{quote_currency}**")
                if 'PAIR_FULL_NAME' in pair_info.columns:
                    st.caption(f"📝 {pair_info.iloc[0]['PAIR_FULL_NAME']}")
            else:
                st.warning(f"⚠️ {base_currency}/{quote_currency} のデータが見つかりません")
                
        else:
            # 複数通貨ペア選択 - マルチセレクト形式
            st.info("💡 BASE通貨とQUOTE通貨の組み合わせを選択してください")
            
            # 複数選択用のインターフェース
            col1, col2 = st.columns(2)
            
            with col1:
                # 利用可能なBASE_CURRENCYのリストを取得
                available_base_currencies = sorted(currency_pairs_df['BASE_CURRENCY_ID'].unique().tolist())
                selected_base_currencies = st.multiselect(
                    "🔵 基軸通貨",
                    available_base_currencies,
                    default=['USD', 'EUR'] if all(c in available_base_currencies for c in ['USD', 'EUR']) else available_base_currencies[:2],
                    help="比較したい基軸通貨を選択（最大3つ）",
                    max_selections=3
                )
            
            with col2:
                # 利用可能なQUOTE_CURRENCYのリストを取得
                available_quote_currencies = sorted(currency_pairs_df['QUOTE_CURRENCY_ID'].unique().tolist())
                selected_quote_currencies = st.multiselect(
                    "🟢 決済通貨",
                    available_quote_currencies,
                    default=['JPY'] if 'JPY' in available_quote_currencies else available_quote_currencies[:1],
                    help="比較したい決済通貨を選択",
                    max_selections=2
                )
            
            # 選択された通貨の組み合わせを生成
            selected_pair_names = []
            for base in selected_base_currencies:
                for quote in selected_quote_currencies:
                    # 該当する通貨ペアが存在するか確認
                    pair_exists = not currency_pairs_df[
                        (currency_pairs_df['BASE_CURRENCY_ID'] == base) & 
                        (currency_pairs_df['QUOTE_CURRENCY_ID'] == quote)
                    ].empty
                    
                    if pair_exists:
                        pair_info = currency_pairs_df[
                            (currency_pairs_df['BASE_CURRENCY_ID'] == base) & 
                            (currency_pairs_df['QUOTE_CURRENCY_ID'] == quote)
                        ].iloc[0]
                        selected_pair_names.append(
                            f"{pair_info['PAIR_DISPLAY']} ({pair_info.get('PAIR_FULL_NAME', pair_info['PAIR_DISPLAY'])})"
                        )
            
            # 選択された通貨ペアを表示
            if selected_pair_names:
                st.success(f"✅ {len(selected_pair_names)}個の通貨ペアを選択中")
                with st.expander("📋 選択中の通貨ペア一覧"):
                    for pair in selected_pair_names:
                        st.write(f"• {pair}")
            else:
                st.warning("⚠️ 有効な通貨ペアの組み合わせがありません")
            
            # 選択された通貨ペアを処理
            selected_pairs = []
            for pair_name in selected_pair_names[:5]:
                pair_display = pair_name.split(' (')[0]
                selected_pairs.append(pair_display)
        
        st.markdown("---")
        
        # 期間選択
        st.subheader("📅 期間選択")
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input(
                "開始日",
                value=datetime.now() - timedelta(days=365),
                max_value=datetime.now().date()
            )
        with col2:
            end_date = st.date_input(
                "終了日",
                value=datetime.now().date(),
                max_value=datetime.now().date()
            )
        
        st.markdown("---")
        
        # テクニカル指標選択（単一通貨ペアモードのみ）
        if analysis_mode == "単一通貨ペア分析":
            st.subheader("📈 テクニカル指標")
            show_technical = st.checkbox("テクニカル指標表示", value=True)
            
            if show_technical:
                st.write("**表示する指標:**")
                show_ma = st.checkbox("移動平均線", value=True)
                show_bb = st.checkbox("ボリンジャーバンド", value=True)
                show_rsi = st.checkbox("RSI", value=True)
                show_macd = st.checkbox("MACD", value=True)
                show_stoch = st.checkbox("ストキャスティクス", value=False)
                show_adx = st.checkbox("ADX", value=False)
            else:
                # テクニカル指標表示がオフの場合、全ての指標をFalseに設定
                show_ma = show_bb = show_rsi = show_macd = show_stoch = show_adx = False
        else:
            # 複数通貨ペア分析の場合
            show_technical = False
            show_ma = show_bb = show_rsi = show_macd = show_stoch = show_adx = False
        
        st.markdown("---")
        
        # AI分析設定
        st.subheader("🤖 AI分析")
        show_ai_analysis = st.checkbox("AI分析を表示", value=True)
        
        if show_ai_analysis and analysis_mode == "単一通貨ペア分析":
            selected_model = st.selectbox(
                "AIモデルを選択",
                AI_COMPLETE_MODELS,
                index=0,
                help="Snowflake Cortex AIモデルを選択"
            )
            
            ai_analysis_type = st.selectbox(
                "分析タイプ",
                ["market_trend", "technical_analysis", "risk_assessment"],
                format_func=lambda x: {
                    "market_trend": "📊 市場トレンド分析",
                    "technical_analysis": "📈 テクニカル分析",
                    "risk_assessment": "⚠️ リスク評価"
                }[x]
            )
        else:
            # AI分析を表示しない場合、またはデフォルト値を設定
            selected_model = AI_COMPLETE_MODELS[0]
            ai_analysis_type = "market_trend"
        
        st.markdown("---")
        st.info("💡 **データソース**\n\nSnowflake Public Data\n\nFX_RATES_TIMESERIES")
    
    # 通貨ペアが選択されているかチェック
    if not selected_pairs:
        st.warning("⚠️ 通貨ペアを選択してください。")
        return
    
    # データ読み込み
    try:
        with st.spinner("データを読み込んでいます..."):
            if analysis_mode == "単一通貨ペア分析":
                df = load_fx_data(start_date, end_date, base_currency, quote_currency)
                if df.empty:
                    st.error("指定期間のデータが見つかりません。")
                    return
                df = calculate_technical_indicators(df)
                all_data = {selected_pairs[0]: df}
            else:
                all_data = load_multiple_fx_data(start_date, end_date, selected_pairs)
                if not all_data:
                    st.error("指定期間のデータが見つかりません。")
                    return
                for pair_name in all_data:
                    all_data[pair_name] = calculate_technical_indicators(all_data[pair_name])
                df = list(all_data.values())[0]
        
        # メイン表示エリア
        if analysis_mode == "単一通貨ペア分析":
            display_single_currency_analysis(df, selected_pairs[0], show_technical, show_ma, show_bb, 
                                            show_rsi, show_macd, show_stoch, show_adx, 
                                            show_ai_analysis, selected_model if show_ai_analysis else None, 
                                            ai_analysis_type if show_ai_analysis else None)
        else:
            display_multiple_currency_comparison(all_data, start_date, end_date)
            
    except Exception as e:
        st.error(f"エラーが発生しました: {str(e)}")
        st.error("データの取得または処理に問題があります。")

def display_single_currency_analysis(df, currency_pair, show_technical, show_ma, show_bb, 
                                     show_rsi, show_macd, show_stoch, show_adx, 
                                     show_ai_analysis, model, analysis_type):
    """単一通貨ペアの詳細分析を表示"""
    
    # メトリクス表示
    col1, col2, col3, col4 = st.columns(4)
    
    current_rate = df['EXCHANGE_RATE'].iloc[-1]
    prev_rate = df['EXCHANGE_RATE'].iloc[-2] if len(df) > 1 else current_rate
    change = current_rate - prev_rate
    change_pct = (change / prev_rate) * 100 if prev_rate != 0 else 0
    
    with col1:
        st.metric(
            f"現在レート ({currency_pair})",
            f"{current_rate:.4f}",
            f"{change:+.4f} ({change_pct:+.2f}%)"
        )
    
    with col2:
        st.metric(
            "期間最高値",
            f"{df['EXCHANGE_RATE'].max():.4f}"
        )
    
    with col3:
        st.metric(
            "期間最安値",
            f"{df['EXCHANGE_RATE'].min():.4f}"
        )
    
    with col4:
        volatility = df['Daily_Return'].std() * 100
        st.metric(
            "ボラティリティ",
            f"{volatility:.2f}%"
        )
    
    st.markdown("---")
    
    # グラフ表示
    st.subheader(f"📈 {currency_pair} チャート分析")
    
    # サブプロット数を計算
    subplot_count = 1
    subplot_titles = [f'{currency_pair} 為替レート']
    row_heights = [0.6]
    
    if show_technical and (show_macd or show_rsi or show_adx):
        subplot_count += 1
        indicators = []
        if show_macd: indicators.append("MACD")
        if show_rsi: indicators.append("RSI")
        if show_adx: indicators.append("ADX")
        subplot_titles.append(f"📊 {' / '.join(indicators)}")
        row_heights.append(0.2)
    
    if show_technical and show_stoch:
        subplot_count += 1
        subplot_titles.append("🔄 ストキャスティクス")
        row_heights.append(0.2)
    
    # row_heightsの正規化
    total_height = sum(row_heights)
    row_heights = [h/total_height for h in row_heights]
    
    fig = make_subplots(
        rows=subplot_count, cols=1,
        subplot_titles=subplot_titles,
        vertical_spacing=0.1,
        row_heights=row_heights
    )
    
    # メイン価格チャート
    fig.add_trace(
        go.Scatter(
            x=df['DATE'],
            y=df['EXCHANGE_RATE'],
            mode='lines',
            name='為替レート',
            line=dict(color='#1f77b4', width=2)
        ),
        row=1, col=1
    )
    
    # 移動平均線
    if show_technical and show_ma:
        for ma, color, name in [('MA_20', 'orange', 'MA20'), ('MA_50', 'red', 'MA50')]:
            if ma in df.columns:
                fig.add_trace(
                    go.Scatter(
                        x=df['DATE'],
                        y=df[ma],
                        mode='lines',
                        name=name,
                        line=dict(color=color, width=1)
                    ),
                    row=1, col=1
                )
    
    # ボリンジャーバンド
    if show_technical and show_bb:
        fig.add_trace(
            go.Scatter(
                x=df['DATE'],
                y=df['BB_Upper'],
                mode='lines',
                name='BB Upper',
                line=dict(color='gray', width=1, dash='dash'),
                showlegend=False
            ),
            row=1, col=1
        )
        
        fig.add_trace(
            go.Scatter(
                x=df['DATE'],
                y=df['BB_Lower'],
                mode='lines',
                name='BB Lower',
                line=dict(color='gray', width=1, dash='dash'),
                fill='tonexty',
                fillcolor='rgba(128,128,128,0.1)',
                showlegend=False
            ),
            row=1, col=1
        )
    
    # 第2サブプロット（テクニカル指標）
    current_row = 2
    if subplot_count >= 2:
        if show_macd:
            fig.add_trace(
                go.Scatter(
                    x=df['DATE'],
                    y=df['MACD'],
                    mode='lines',
                    name='MACD',
                    line=dict(color='blue', width=1)
                ),
                row=current_row, col=1
            )
            
            fig.add_trace(
                go.Scatter(
                    x=df['DATE'],
                    y=df['MACD_Signal'],
                    mode='lines',
                    name='Signal',
                    line=dict(color='red', width=1)
                ),
                row=current_row, col=1
            )
            
            fig.add_trace(
                go.Bar(
                    x=df['DATE'],
                    y=df['MACD_Histogram'],
                    name='MACD Hist',
                    marker_color='green',
                    opacity=0.6
                ),
                row=current_row, col=1
            )
        
        if show_rsi:
            fig.add_trace(
                go.Scatter(
                    x=df['DATE'],
                    y=df['RSI'],
                    mode='lines',
                    name='RSI',
                    line=dict(color='purple', width=2)
                ),
                row=current_row, col=1
            )
            fig.add_hline(y=70, line_dash="dash", line_color="red", row=current_row, col=1)
            fig.add_hline(y=30, line_dash="dash", line_color="green", row=current_row, col=1)
        
        if show_adx and 'ADX' in df.columns:
            fig.add_trace(
                go.Scatter(
                    x=df['DATE'],
                    y=df['ADX'],
                    mode='lines',
                    name='ADX',
                    line=dict(color='orange', width=2)
                ),
                row=current_row, col=1
            )
    
    # 第3サブプロット（ストキャスティクス）
    if subplot_count >= 3 and show_stoch:
        current_row = 3
        fig.add_trace(
            go.Scatter(
                x=df['DATE'],
                y=df['Stoch_K'],
                mode='lines',
                name='Stoch %K',
                line=dict(color='blue', width=1)
            ),
            row=current_row, col=1
        )
        
        fig.add_trace(
            go.Scatter(
                x=df['DATE'],
                y=df['Stoch_D'],
                mode='lines',
                name='Stoch %D',
                line=dict(color='red', width=1)
            ),
            row=current_row, col=1
        )
        
        fig.add_hline(y=80, line_dash="dash", line_color="red", row=current_row, col=1)
        fig.add_hline(y=20, line_dash="dash", line_color="green", row=current_row, col=1)
    
    # レイアウト更新
    fig.update_layout(
        height=700,
        showlegend=True,
        hovermode='x unified'
    )
    
    fig.update_xaxes(title_text="日付", row=subplot_count, col=1)
    fig.update_yaxes(title_text="レート", row=1, col=1)
    
    if subplot_count >= 2:
        fig.update_yaxes(title_text="指標値", row=2, col=1)
        if show_rsi:
            fig.update_yaxes(range=[0, 100], row=2, col=1)
    
    if subplot_count >= 3:
        fig.update_yaxes(title_text="ストキャスティクス", row=3, col=1)
        fig.update_yaxes(range=[0, 100], row=3, col=1)
    
    st.plotly_chart(fig, use_container_width=True)
    
    # 統計分析セクション
    st.markdown("---")
    st.subheader("📊 統計分析")
    
    col1, col2 = st.columns(2)
    
    with col1:
        with st.container(border=True):
            st.write(f"**{currency_pair} 基本統計量**")
            stats_df = pd.DataFrame({
                '統計量': ['平均', '中央値', '標準偏差', '最小値', '最大値', '歪度', '尖度'],
                '値': [
                    f"{df['EXCHANGE_RATE'].mean():.4f}",
                    f"{df['EXCHANGE_RATE'].median():.4f}",
                    f"{df['EXCHANGE_RATE'].std():.4f}",
                    f"{df['EXCHANGE_RATE'].min():.4f}",
                    f"{df['EXCHANGE_RATE'].max():.4f}",
                    f"{df['EXCHANGE_RATE'].skew():.2f}",
                    f"{df['EXCHANGE_RATE'].kurtosis():.2f}"
                ]
            })
            st.dataframe(stats_df, use_container_width=True, hide_index=True)
    
    with col2:
        with st.container(border=True):
            st.write("**テクニカル指標サマリー**")
            
            tech_data = []
            latest_data = df.iloc[-1]
            
            if 'RSI' in df.columns and pd.notna(latest_data['RSI']):
                rsi_signal = "買われすぎ" if latest_data['RSI'] > 70 else "売られすぎ" if latest_data['RSI'] < 30 else "中立"
                tech_data.append(["RSI", f"{latest_data['RSI']:.1f}", rsi_signal])
            
            if 'Stoch_K' in df.columns and pd.notna(latest_data['Stoch_K']):
                stoch_signal = "買われすぎ" if latest_data['Stoch_K'] > 80 else "売られすぎ" if latest_data['Stoch_K'] < 20 else "中立"
                tech_data.append(["Stochastic %K", f"{latest_data['Stoch_K']:.1f}", stoch_signal])
            
            if 'ADX' in df.columns and pd.notna(latest_data['ADX']):
                adx_signal = "強いトレンド" if latest_data['ADX'] > 25 else "弱いトレンド" if latest_data['ADX'] > 20 else "レンジ相場"
                tech_data.append(["ADX", f"{latest_data['ADX']:.1f}", adx_signal])
            
            if tech_data:
                tech_df = pd.DataFrame(tech_data, columns=["指標", "値", "シグナル"])
                st.dataframe(tech_df, use_container_width=True, hide_index=True)
            else:
                st.info("テクニカル指標を有効にしてください")
    
    # AI分析セクション
    if show_ai_analysis:
        st.markdown("---")
        st.subheader("🤖 AI分析")
        
        with st.spinner("AI分析を実行中..."):
            ai_result = get_ai_analysis(df, analysis_type, currency_pair, model)
        
        with st.container(border=True):
            st.write(f"**{currency_pair} AI分析結果**")
            # AI分析結果の表示（Markdown記号を削除して読みやすく表示）
            if ai_result:
                # Markdown見出し記号を削除して、適切にフォーマット
                formatted_result = ai_result.replace("#### ", "").replace("### ", "").replace("## ", "").replace("# ", "")
                # 短いサブタイトルのみを太字に変換（50文字以内の行のみ対象）
                formatted_result = re.sub(r'^(\d+\.\s+.{1,50})$', r'**\1**', formatted_result, flags=re.MULTILINE)
                st.markdown(formatted_result)
    
    # データ詳細
    with st.expander("📋 データ詳細を表示"):
        st.write(f"**{currency_pair} 最新データ (直近20日)**")
        
        display_columns = ['DATE', 'EXCHANGE_RATE', 'Daily_Return']
        if show_ma:
            display_columns.extend(['MA_20', 'MA_50'])
        if show_rsi and 'RSI' in df.columns:
            display_columns.append('RSI')
        
        available_columns = [col for col in display_columns if col in df.columns]
        latest_data = df[available_columns].tail(20).copy()
        
        if 'Daily_Return' in latest_data.columns:
            latest_data['Daily_Return'] = latest_data['Daily_Return'].map(
                lambda x: f"{x*100:.2f}%" if pd.notna(x) else "N/A"
            )
        
        st.dataframe(latest_data, use_container_width=True)
        
        csv = df.to_csv(index=False)
        st.download_button(
            label="📥 データをCSVでダウンロード",
            data=csv,
            file_name=f"{currency_pair.replace('/', '_')}_analysis.csv",
            mime="text/csv"
        )

def display_multiple_currency_comparison(all_data, start_date, end_date):
    """複数通貨ペアの比較分析を表示"""
    
    st.subheader("📊 通貨ペア別サマリー")
    
    # メトリクス表示
    metrics_data = []
    for pair_name, pair_df in all_data.items():
        if not pair_df.empty:
            current_rate = pair_df['EXCHANGE_RATE'].iloc[-1]
            prev_rate = pair_df['EXCHANGE_RATE'].iloc[-2] if len(pair_df) > 1 else current_rate
            change = current_rate - prev_rate
            change_pct = (change / prev_rate) * 100 if prev_rate != 0 else 0
            volatility = pair_df['Daily_Return'].std() * 100
            
            metrics_data.append({
                "通貨ペア": pair_name,
                "現在レート": f"{current_rate:.4f}",
                "前日比": f"{change:+.4f}",
                "変動率": f"{change_pct:+.2f}%",
                "期間最高値": f"{pair_df['EXCHANGE_RATE'].max():.4f}",
                "期間最安値": f"{pair_df['EXCHANGE_RATE'].min():.4f}",
                "ボラティリティ": f"{volatility:.2f}%"
            })
    
    if metrics_data:
        metrics_df = pd.DataFrame(metrics_data)
        st.dataframe(metrics_df, use_container_width=True, hide_index=True)
    
    st.markdown("---")
    st.subheader("📈 通貨ペア比較チャート")
    
    # 比較チャート作成
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=('為替レート比較 (正規化)', 'ボラティリティ比較'),
        vertical_spacing=0.2,
        row_heights=[0.7, 0.3]
    )
    
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
    
    for i, (pair_name, pair_df) in enumerate(all_data.items()):
        if not pair_df.empty:
            # 正規化（初期値を100とする）
            initial_rate = pair_df['EXCHANGE_RATE'].iloc[0]
            normalized_rate = (pair_df['EXCHANGE_RATE'] / initial_rate) * 100
            
            fig.add_trace(
                go.Scatter(
                    x=pair_df['DATE'],
                    y=normalized_rate,
                    mode='lines',
                    name=pair_name,
                    line=dict(color=colors[i % len(colors)], width=2)
                ),
                row=1, col=1
            )
            
            # ボラティリティ
            if 'Volatility' in pair_df.columns:
                fig.add_trace(
                    go.Scatter(
                        x=pair_df['DATE'],
                        y=pair_df['Volatility'] * 100,
                        mode='lines',
                        name=f'{pair_name} Vol',
                        line=dict(color=colors[i % len(colors)], width=1, dash='dash'),
                        showlegend=False
                    ),
                    row=2, col=1
                )
    
    fig.update_layout(
        height=600,
        showlegend=True,
        hovermode='x unified'
    )
    
    fig.update_xaxes(title_text="日付", row=2, col=1)
    fig.update_yaxes(title_text="正規化レート (初期値=100)", row=1, col=1)
    fig.update_yaxes(title_text="ボラティリティ (%)", row=2, col=1)
    
    st.plotly_chart(fig, use_container_width=True)
    
    # 相関分析
    st.markdown("---")
    st.subheader("🔗 相関分析")
    
    if len(all_data) > 1:
        correlation_data = {}
        for pair_name, pair_df in all_data.items():
            if not pair_df.empty:
                correlation_data[pair_name] = pair_df.set_index('DATE')['EXCHANGE_RATE']
        
        corr_df = pd.DataFrame(correlation_data)
        corr_matrix = corr_df.corr()
        
        col1, col2 = st.columns(2)
        
        with col1:
            with st.container(border=True):
                st.write("**相関係数行列**")
                st.dataframe(corr_matrix.round(3), use_container_width=True)
        
        with col2:
            with st.container(border=True):
                st.write("**相関ヒートマップ**")
            
            fig_corr = px.imshow(
                corr_matrix,
                color_continuous_scale="RdBu",
                aspect="auto",
                text_auto='.2f'
            )
            fig_corr.update_layout(height=400)
            st.plotly_chart(fig_corr, use_container_width=True)
    
    # 統計分析
    st.markdown("---")
    st.subheader("📊 通貨ペア別統計分析")
    
    with st.container(border=True):
        stats_data = []
        for pair_name, pair_df in all_data.items():
            if not pair_df.empty:
                stats_data.append({
                    "通貨ペア": pair_name,
                    "平均": f"{pair_df['EXCHANGE_RATE'].mean():.4f}",
                    "標準偏差": f"{pair_df['EXCHANGE_RATE'].std():.4f}",
                    "最小値": f"{pair_df['EXCHANGE_RATE'].min():.4f}",
                    "最大値": f"{pair_df['EXCHANGE_RATE'].max():.4f}",
                    "変動係数": f"{(pair_df['EXCHANGE_RATE'].std() / pair_df['EXCHANGE_RATE'].mean()):.4f}",
                    "歪度": f"{pair_df['EXCHANGE_RATE'].skew():.2f}"
                })
        
        if stats_data:
            stats_df = pd.DataFrame(stats_data)
            st.dataframe(stats_df, use_container_width=True, hide_index=True)

# フッター
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #666; margin-top: 2rem;'>
        <p>💱 為替分析システム | Powered by Streamlit & Snowflake</p>
        <p style='font-size: 0.8rem;'>
        Data Source: SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.FX_RATES_TIMESERIES
        </p>
        <p style='font-size: 0.7rem; margin-top: 0.5rem;'>
        </p>
    </div>
    """,
    unsafe_allow_html=True
)

if __name__ == "__main__":
    main()