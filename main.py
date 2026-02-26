import discord
import pandas as pd
import io
import os
import re
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials

# =============================
# Discord設定
# =============================
TOKEN = os.getenv("DISCORD_TOKEN")

intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)

# =============================
# Google Sheets設定
# =============================
SPREADSHEET_NAME = "妖精CSマンスリーランキング"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

def get_sheet():
    creds_dict = eval(os.getenv("GOOGLE_CREDENTIALS"))
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    gc = gspread.authorize(creds)
    return gc.open(SPREADSHEET_NAME)

# =============================
# 基礎ポイント計算
# =============================
def get_base_point(rank):
    try:
        rank = int(rank)
    except:
        return 3

    if rank == 1:
        return 20
    elif rank == 2:
        return 15
    elif 3 <= rank <= 4:
        return 10
    elif 5 <= rank <= 8:
        return 7
    elif 9 <= rank <= 16:
        return 5
    elif 17 <= rank <= 32:
        return 4
    else:
        return 3

# =============================
# CSV読み込み（UTF-8 / SJIS対応）
# =============================
def read_csv_safely(file_bytes):
    encodings = ["utf-8-sig", "utf-8", "cp932", "shift-jis"]
    for enc in encodings:
        try:
            return pd.read_csv(io.BytesIO(file_bytes), encoding=enc)
        except:
            continue
    raise Exception("CSVの文字コードを判定できませんでした。UTF-8またはSJISで保存してください。")

# =============================
# NaN安全変換（JSON完全対応）
# =============================
def safe_value(v):
    if pd.isna(v):
        return ""
    if isinstance(v, float):
        if pd.isna(v):
            return ""
        if v == float("inf") or v == float("-inf"):
            return ""
    return v

# =============================
# メイン処理
# =============================
@client.event
async def on_message(message):
    if message.author.bot:
        return

    if not message.attachments:
        return

    attachment = message.attachments[0]

    if not attachment.filename.endswith(".csv"):
        return

    await message.channel.send(f"受信ファイル名: {attachment.filename}")
    await message.channel.send("CSVを受け取りました。集計します...")

    file_bytes = await attachment.read()

    # CSV読み込み
    try:
        df = read_csv_safely(file_bytes)
    except Exception as e:
        await message.channel.send(str(e))
        return

    # NaN完全除去
    df = df.fillna("")

    # 必須列チェック
    required_columns = ["順位", "識別番号", "氏名"]
    for col in required_columns:
        if col not in df.columns:
            await message.channel.send(f"エラー：'{col}' 列が見つかりません。")
            return

    # =============================
    # 日付抽出（ファイル名の8桁数字）
    # =============================
    match = re.search(r"\d{8}", attachment.filename)
    if not match:
        await message.channel.send("ファイル名から日付を取得できませんでした。")
        return

    date_str = match.group(0)

    try:
        event_date = datetime.strptime(date_str, "%Y%m%d")
    except ValueError:
        await message.channel.send("日付形式が不正です。")
        return

    month_str = event_date.strftime("%Y-%m")
    date_display = event_date.strftime("%Y-%m-%d")

    participant_count = len(df)

    # ポイント計算
    def calc_point(row):
        base = get_base_point(row["順位"])
        return base * participant_count

    df["獲得pt"] = df.apply(calc_point, axis=1)
    df["開催日"] = date_display
    df["月"] = month_str
    df["参加人数"] = participant_count

    spreadsheet = get_sheet()

    # =============================
    # 大会ログシート（自動修復対応）
    # =============================
    try:
        log_sheet = spreadsheet.worksheet("大会ログ")
        headers = log_sheet.row_values(1)

        expected_headers = ["開催日","月","識別番号","氏名","順位","参加人数","獲得pt"]

        if headers != expected_headers:
            log_sheet.clear()
            log_sheet.append_row(expected_headers)

    except:
        log_sheet = spreadsheet.add_worksheet(title="大会ログ", rows=1000, cols=10)
        log_sheet.append_row(["開催日","月","識別番号","氏名","順位","参加人数","獲得pt"])

    # ログ追記（NaN完全排除）
    for _, row in df.iterrows():
        values = [
            safe_value(row["開催日"]),
            safe_value(row["月"]),
            safe_value(row["識別番号"]),
            safe_value(row["氏名"]),
            safe_value(row["順位"]),
            safe_value(row["参加人数"]),
            safe_value(row["獲得pt"])
        ]
        log_sheet.append_row(values)

    # =============================
    # 月別集計
    # =============================
    records = log_sheet.get_all_records()
    log_df = pd.DataFrame(records)

    if "月" not in log_df.columns:
        await message.channel.send("大会ログの列構造に問題があります。")
        return

    month_df = log_df[log_df["月"] == month_str]

    grouped = (
        month_df.groupby("識別番号")
        .agg({
            "氏名": "first",
            "獲得pt": "sum"
        })
        .reset_index()
    )

    grouped = grouped.sort_values(by="獲得pt", ascending=False)
    grouped["順位"] = range(1, len(grouped) + 1)

    # 月シート取得 or 作成
    try:
        month_sheet = spreadsheet.worksheet(month_str)
        month_sheet.clear()
    except:
        month_sheet = spreadsheet.add_worksheet(title=month_str, rows=1000, cols=10)

    month_sheet.append_row(["順位","識別番号","氏名","合計pt"])

    for _, row in grouped.iterrows():
        values = [
            safe_value(row["順位"]),
            safe_value(row["識別番号"]),
            safe_value(row["氏名"]),
            safe_value(row["獲得pt"])
        ]
        month_sheet.append_row(values)

    await message.channel.send(f"{month_str} のランキングを更新しました！")

client.run(TOKEN)
