import discord
import pandas as pd
import io
import os
import json
import gspread
from google.oauth2.service_account import Credentials

# =========================
# 環境変数
# =========================
TOKEN = os.getenv("DISCORD_TOKEN")
SPREADSHEET_NAME = "妖精CSマンスリーランキング"
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")

if TOKEN is None:
    raise ValueError("DISCORD_TOKEN is not set!")

if GOOGLE_CREDENTIALS is None:
    raise ValueError("GOOGLE_CREDENTIALS is not set!")

# =========================
# Google Sheet 接続
# =========================
def get_sheet():
    creds_json = json.loads(GOOGLE_CREDENTIALS)

    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = Credentials.from_service_account_info(creds_json, scopes=scope)
    gc = gspread.authorize(creds)

    spreadsheet = gc.open(SPREADSHEET_NAME)
    sheet = spreadsheet.sheet1
    return sheet


# =========================
# Discord Bot 設定
# =========================
intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)


@client.event
async def on_ready():
    print(f"Logged in as {client.user}")


@client.event
async def on_message(message):
    if message.author == client.user:
        return

    if message.attachments:
        attachment = message.attachments[0]

        if attachment.filename.endswith(".csv"):
            await message.channel.send("CSVを受け取りました。集計します...")

            file_bytes = await attachment.read()

            # =========================
            # CSV読み込み（文字コード自動対応）
            # =========================
            try:
                df = pd.read_csv(io.BytesIO(file_bytes), encoding="utf-8")
            except UnicodeDecodeError:
                df = pd.read_csv(io.BytesIO(file_bytes), encoding="cp932")

            # =========================
            # 必要列チェック
            # =========================
            required_columns = ["識別番号", "プレイヤー名", "スコア"]
            for col in required_columns:
                if col not in df.columns:
                    await message.channel.send(f"エラー：'{col}' 列が見つかりません。")
                    return

            # =========================
            # 識別番号で自動合算
            # =========================
            grouped = (
                df.groupby("識別番号")
                .agg({
                    "プレイヤー名": "first",
                    "スコア": "sum"
                })
                .reset_index()
            )

            # =========================
            # ランキング作成
            # =========================
            grouped = grouped.sort_values(by="スコア", ascending=False)
            grouped["順位"] = range(1, len(grouped) + 1)

            # =========================
            # Google Sheetへ保存
            # =========================
            sheet = get_sheet()
            sheet.clear()
            sheet.update([grouped.columns.values.tolist()] + grouped.values.tolist())

            await message.channel.send("✅ 集計完了！スプレッドシートに保存しました。")


# =========================
# Bot起動
# =========================
client.run(TOKEN)
