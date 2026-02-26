import discord
import pandas as pd
import os
import io
import json
import datetime
import gspread
from google.oauth2.service_account import Credentials

# =========================
# Discord設定
# =========================

TOKEN = os.getenv("DISCORD_TOKEN")

intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)

# =========================
# Google Sheets接続
# =========================

SPREADSHEET_NAME = "妖精CSマンスリーランキング"
SHEET_NAME = "monthly"

def get_sheet():
    creds_dict = json.loads(os.getenv("GOOGLE_CREDENTIALS"))
    creds = Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)
    spreadsheet = gc.open(SPREADSHEET_NAME)
    return spreadsheet.worksheet(SHEET_NAME)

# =========================
# ポイント計算
# =========================

def base_point(rank):
    if rank == 1:
        return 20
    elif rank == 2:
        return 15
    elif rank == 3:
        return 10
    elif 5 <= rank <= 8:
        return 7
    elif 9 <= rank <= 16:
        return 5
    elif 17 <= rank <= 32:
        return 4
    elif 33 <= rank <= 64:
        return 3
    else:
        return 3

# =========================
# Bot起動
# =========================

@client.event
async def on_ready():
    print(f"Logged in as {client.user}")

@client.event
async def on_message(message):

    if message.author.bot:
        return

    if message.attachments:

        for attachment in message.attachments:

            if attachment.filename.endswith(".csv"):

                await message.channel.send("CSVを受け取りました。集計します...")

                # CSV読み込み
                file = await attachment.read()
                try:
                df = pd.read_csv(io.BytesIO(file), encoding="utf-8")
                except UnicodeDecodeError:
                df = pd.read_csv(io.BytesIO(file), encoding="cp932")

                participants = len(df)

                df["順位_int"] = df["順位"].fillna(64).astype(int)
                df["基礎ポイント"] = df["順位_int"].apply(base_point)
                df["獲得ポイント"] = df["基礎ポイント"] * participants

                # 今月
                now = datetime.datetime.now()
                month_str = now.strftime("%Y-%m")

                # 必要列だけ抽出
                upload_df = df[["識別番号", "氏名", "獲得ポイント"]].copy()
                upload_df["月"] = month_str

                # Google Sheetへ追加
                sheet = get_sheet()
                sheet.append_rows(upload_df.values.tolist())

                # ===== 月別合算 =====

                all_data = sheet.get_all_records()
                all_df = pd.DataFrame(all_data)

                month_df = all_df[all_df["月"] == month_str]

                ranking = (
                    month_df
                    .groupby(["識別番号", "氏名"])["獲得ポイント"]
                    .sum()
                    .reset_index()
                )

                ranking = ranking.sort_values(
                    by="獲得ポイント",
                    ascending=False
                ).reset_index(drop=True)

                ranking["順位"] = ranking.index + 1

                # ===== Discord出力 =====

                result_text = f"【{month_str} マンスリーランキング】\n\n"

                for _, row in ranking.iterrows():
                    result_text += (
                        f"{row['順位']}位 "
                        f"{row['氏名']} "
                        f"{row['獲得ポイント']}pt\n"
                    )

                # 長文対策
                if len(result_text) > 1900:
                    chunks = [result_text[i:i+1900] for i in range(0, len(result_text), 1900)]
                    for chunk in chunks:
                        await message.channel.send(chunk)
                else:
                    await message.channel.send(result_text)

client.run(TOKEN)
