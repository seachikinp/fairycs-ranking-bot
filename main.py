import discord
import pandas as pd
import io
import os
import re
import threading
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
from flask import Flask

# =========================
# ダミーWebサーバー（Render無料対策）
# =========================
app = Flask(__name__)

@app.route("/")
def home():
    return "Bot is running"

def run_web():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

threading.Thread(target=run_web).start()

# =========================
# Discord設定
# =========================
TOKEN = os.getenv("DISCORD_TOKEN")

intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)

SPREADSHEET_NAME = "妖精CSマンスリーランキング"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# =========================
# Google Sheets接続
# =========================
def get_sheet():
    creds_dict = eval(os.getenv("GOOGLE_CREDENTIALS"))
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    gc = gspread.authorize(creds)
    return gc.open(SPREADSHEET_NAME)

# =========================
# ポイント計算
# =========================
def get_base_point(rank):
    try:
        rank = int(rank)
    except:
        return 3

    if rank == 1: return 20
    if rank == 2: return 15
    if 3 <= rank <= 4: return 10
    if 5 <= rank <= 8: return 7
    if 9 <= rank <= 16: return 5
    if 17 <= rank <= 32: return 4
    return 3

# =========================
# CSV安全読み込み
# =========================
def read_csv_safely(file_bytes):
    encodings = ["utf-8-sig", "utf-8", "cp932", "shift-jis"]
    for enc in encodings:
        try:
            return pd.read_csv(io.BytesIO(file_bytes), encoding=enc)
        except:
            continue
    raise Exception("CSVの文字コードを判定できませんでした。")

def clean_df(df):
    df = df.fillna("")
    df = df.replace([float("inf"), float("-inf")], "")
    return df

# =========================
# Discordイベント
# =========================
@client.event
async def on_message(message):
    if message.author.bot:
        return

    if not message.attachments:
        return

    attachment = message.attachments[0]
    if not attachment.filename.endswith(".csv"):
        return

    await message.channel.send("CSVを受け取りました。集計します...")

    file_bytes = await attachment.read()
    df = read_csv_safely(file_bytes)
    df = clean_df(df)

    required_columns = ["順位", "識別番号", "氏名"]
    for col in required_columns:
        if col not in df.columns:
            await message.channel.send(f"'{col}' 列が見つかりません。")
            return

    match = re.search(r"\d{8}", attachment.filename)
    if not match:
        await message.channel.send("ファイル名に日付（YYYYMMDD）が見つかりません。")
        return

    event_date = datetime.strptime(match.group(0), "%Y%m%d")
    month_str = event_date.strftime("%Y-%m")
    date_display = event_date.strftime("%Y-%m-%d")

    participant_count = len(df)

    df["獲得pt"] = df["順位"].apply(lambda r: get_base_point(r) * participant_count)
    df["開催日"] = date_display
    df["月"] = month_str
    df["参加人数"] = participant_count

    spreadsheet = get_sheet()

    # =========================
    # 大会ログ
    # =========================
    try:
        log_sheet = spreadsheet.worksheet("大会ログ")
    except:
        log_sheet = spreadsheet.add_worksheet(title="大会ログ", rows=2000, cols=10)
        log_sheet.append_row(["開催日","月","識別番号","氏名","順位","参加人数","獲得pt"])

    log_values = df[["開催日","月","識別番号","氏名","順位","参加人数","獲得pt"]].values.tolist()
    log_sheet.append_rows(log_values)

    # =========================
    # 月別集計
    # =========================
    records = log_sheet.get_all_records()
    log_df = pd.DataFrame(records)

    month_df = log_df[log_df["月"] == month_str]

    grouped = (
        month_df.groupby("識別番号")
        .agg({"氏名": "first", "獲得pt": "sum"})
        .reset_index()
    )

    grouped = grouped.sort_values(by="獲得pt", ascending=False)
    grouped["順位"] = range(1, len(grouped) + 1)

    try:
        month_sheet = spreadsheet.worksheet(month_str)
        month_sheet.clear()
    except:
        month_sheet = spreadsheet.add_worksheet(title=month_str, rows=2000, cols=10)

    header = [["順位","識別番号","氏名","合計pt"]]
    data = grouped[["順位","識別番号","氏名","獲得pt"]].values.tolist()

    month_sheet.update("A1", header + data)
    
    # =========================
    # 上位15位をEmbed表示
    # =========================
    top15 = grouped.head(15)

    embed = discord.Embed(
        title=f"🏆 {month_str} マンスリーランキング TOP15",
        color=0xFFD700
    )

    description_lines = []

    for _, row in top15.iterrows():
        rank = row["順位"]
        name = row["氏名"]
        pt = row["獲得pt"]

        if rank == 1:
            line = f"🥇 **1位** {name} - {pt}pt"
        elif rank == 2:
            line = f"🥈 **2位** {name} - {pt}pt"
        elif rank == 3:
            line = f"🥉 **3位** {name} - {pt}pt"
        else:
            line = f"{rank}位 {name} - {pt}pt"

        description_lines.append(line)

    embed.description = "\n".join(description_lines)

    await message.channel.send(embed=embed)
    await message.channel.send(f"{month_str} のランキングを更新しました！")

# =========================
# 起動
# =========================
client.run(TOKEN)
