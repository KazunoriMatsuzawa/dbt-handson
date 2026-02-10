"""
================================================================================
ダミーデータ生成スクリプト：Webアクセスログ分析用
================================================================================

このスクリプトは、以下の3つのテーブル用のダミーデータを生成します：
- raw_events.csv: イベントログ（50万件）
- users.csv: ユーザーマスタ（1万件）
- sessions.csv: セッションサマリ（10万件）

生成されたCSVファイルは、Snowflakeにロードするために使用されます。

使用方法：
    python generate_data.py

生成データの特徴：
- リアルなログデータを再現（時系列、分布など）
- 外部キー制約を満たす（user_id、session_idの整合性）
- 複数国のデータ（主にUS、JP、GB、DE、FR等）
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import string
from pathlib import Path

# ==========================================
# 設定
# ==========================================
RANDOM_SEED = 42
np.random.seed(RANDOM_SEED)
random.seed(RANDOM_SEED)

NUM_USERS = 10_000
NUM_SESSIONS = 100_000
NUM_EVENTS = 500_000
NUM_DAYS = 90  # 過去90日間のデータ

# イベント種別の分布（%）
EVENT_TYPE_DISTRIBUTION = {
    'page_view': 0.50,   # 50%
    'click': 0.25,       # 25%
    'add_to_cart': 0.10, # 10%
    'checkout': 0.08,    # 8%
    'purchase': 0.05,    # 5%
    'sign_up': 0.02      # 2%
}

DEVICE_TYPES = ['desktop', 'mobile', 'tablet']
DEVICE_DISTRIBUTION = {'desktop': 0.40, 'mobile': 0.45, 'tablet': 0.15}

COUNTRIES = ['US', 'JP', 'GB', 'DE', 'FR', 'CA', 'AU', 'SG', 'IN', 'BR']
COUNTRY_DISTRIBUTION = {
    'US': 0.30, 'JP': 0.25, 'GB': 0.15, 'DE': 0.10,
    'FR': 0.05, 'CA': 0.05, 'AU': 0.03, 'SG': 0.03,
    'IN': 0.02, 'BR': 0.02
}

PLAN_TYPES = ['free', 'premium']
PLAN_DISTRIBUTION = {'free': 0.85, 'premium': 0.15}

PAGE_URLS = [
    '/home',
    '/products',
    '/products/1',
    '/products/2',
    '/products/3',
    '/cart',
    '/checkout',
    '/about',
    '/contact',
    '/blog',
    '/blog/post-1',
    '/blog/post-2'
]


# ==========================================
# ユーティリティ関数
# ==========================================

def generate_session_id():
    """セッションIDを生成"""
    return 'session_' + ''.join(random.choices(string.ascii_lowercase + string.digits, k=16))


def generate_users():
    """usersテーブルのデータを生成"""
    print("Generating users table...")

    # 登録日：過去180日以内
    signup_dates = [
        (datetime.now() - timedelta(days=random.randint(0, 180))).date()
        for _ in range(NUM_USERS)
    ]

    users = pd.DataFrame({
        'user_id': range(1, NUM_USERS + 1),
        'signup_date': signup_dates,
        'country': np.random.choice(
            list(COUNTRY_DISTRIBUTION.keys()),
            size=NUM_USERS,
            p=list(COUNTRY_DISTRIBUTION.values())
        ),
        'plan_type': np.random.choice(
            list(PLAN_DISTRIBUTION.keys()),
            size=NUM_USERS,
            p=list(PLAN_DISTRIBUTION.values())
        ),
        'is_active': np.random.choice([True, False], size=NUM_USERS, p=[0.8, 0.2])
    })

    return users


def generate_sessions(users):
    """sessionsテーブルのデータを生成"""
    print("Generating sessions table...")

    base_date = datetime.now() - timedelta(days=NUM_DAYS)

    sessions_data = []
    for _ in range(NUM_SESSIONS):
        user_id = random.choice(users['user_id'].values)
        session_start = base_date + timedelta(
            days=random.randint(0, NUM_DAYS - 1),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )
        session_duration = timedelta(minutes=random.randint(1, 120))
        session_end = session_start + session_duration
        page_views = max(1, np.random.exponential(scale=3, size=1)[0].astype(int) + 1)

        sessions_data.append({
            'session_id': generate_session_id(),
            'user_id': user_id,
            'session_start': session_start,
            'session_end': session_end,
            'page_views': page_views,
            'device_type': np.random.choice(
                list(DEVICE_DISTRIBUTION.keys()),
                p=list(DEVICE_DISTRIBUTION.values())
            )
        })

    sessions = pd.DataFrame(sessions_data)
    return sessions


def generate_events(users, sessions):
    """raw_eventsテーブルのデータを生成"""
    print("Generating raw_events table...")

    # パフォーマンス改善：user_id → country のルックアップテーブルを事前作成
    user_country_map = dict(zip(users['user_id'], users['country']))

    # イベント種別の選択肢を事前準備
    event_type_keys = list(EVENT_TYPE_DISTRIBUTION.keys())
    event_type_probs = list(EVENT_TYPE_DISTRIBUTION.values())

    events_data = []
    event_id = 1

    # セッションごとにイベントを生成
    for idx, session in sessions.iterrows():
        # セッション内のイベント数（page_viewsから決定）
        num_events_in_session = session['page_views'] + np.random.poisson(lam=2)
        session_duration = (session['session_end'] - session['session_start']).total_seconds()

        for _ in range(num_events_in_session):
            # イベントはセッション期間内でランダムに分布
            time_offset = random.uniform(0, session_duration)
            event_timestamp = session['session_start'] + timedelta(seconds=time_offset)

            event_type = np.random.choice(event_type_keys, p=event_type_probs)

            events_data.append({
                'event_id': event_id,
                'user_id': session['user_id'],
                'session_id': session['session_id'],
                'event_type': event_type,
                'page_url': random.choice(PAGE_URLS),
                'event_timestamp': event_timestamp,
                'device_type': session['device_type'],
                'country': user_country_map[session['user_id']]
            })
            event_id += 1

            # 目標イベント数に達したら中断
            if event_id > NUM_EVENTS:
                break

        if event_id > NUM_EVENTS:
            break

        if (idx + 1) % 10000 == 0:
            print(f"  Generated {idx + 1} sessions...")

    events = pd.DataFrame(events_data[:NUM_EVENTS])
    return events


def save_to_csv(df, filepath):
    """DataFrameをCSVに保存"""
    print(f"Saving {filepath}...")
    df.to_csv(filepath, index=False, date_format='%Y-%m-%d %H:%M:%S')
    print(f"  ✓ {filepath} ({len(df):,} rows)")


# ==========================================
# メイン処理
# ==========================================

if __name__ == '__main__':
    print("=" * 80)
    print("ダミーデータ生成開始")
    print("=" * 80)
    print()

    # ディレクトリの確認・作成
    output_dir = Path(__file__).parent

    try:
        # ユーザーデータ生成
        users = generate_users()
        save_to_csv(users, output_dir / 'users.csv')
        print()

        # セッションデータ生成
        sessions = generate_sessions(users)
        save_to_csv(sessions, output_dir / 'sessions.csv')
        print()

        # イベントデータ生成
        events = generate_events(users, sessions)
        save_to_csv(events, output_dir / 'raw_events.csv')
        print()

        # 統計情報の表示
        print("=" * 80)
        print("生成データの統計")
        print("=" * 80)
        print(f"Users: {len(users):,} rows")
        print(f"  - Plan Type 分布:\n{users['plan_type'].value_counts()}")
        print(f"  - Country 分布:\n{users['country'].value_counts()}")
        print()
        print(f"Sessions: {len(sessions):,} rows")
        print(f"  - Device Type 分布:\n{sessions['device_type'].value_counts()}")
        print(f"  - Page Views 統計:\n{sessions['page_views'].describe()}")
        print()
        print(f"Events: {len(events):,} rows")
        print(f"  - Event Type 分布:\n{events['event_type'].value_counts()}")
        print(f"  - Device Type 分布:\n{events['device_type'].value_counts()}")
        print()
        print("=" * 80)
        print("✓ データ生成完了")
        print("=" * 80)

    except Exception as e:
        print(f"エラーが発生しました: {e}")
        raise
