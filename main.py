import discord
import pandas as pd
import io
import os
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials

TOKEN = os.getenv("TOKEN")

SPREADSHEET_NAME = "妖精CSマンスリーランキング"
SHEET_NAME = "monthly"

intents = discord.Intents.default()
intents.message_content = True
bot = discord.Client(intents=intents)

def get_base_point(rank):
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
    else:
        return 3

def get_sheet():
    creds_dict = eval(os.getenv("GOOGLE_CREDENTIALS"))
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(credentials)
    return gc.open(SPREADSHEET_NAME).worksheet(SHEET_NAME)

@bot.event
async def on_message(message):
    if message.author.bot:
        return

    if message.attachments:
        for attachment in message.attachments:
            if attachment.filename.endswith(".csv"):

                file = await attachment.read()
                df = pd.read_csv(io.BytesIO(file), encoding="cp932")

                participants = len(df)

                df["順位"] = df["順位"].fillna(64).astype(int)
                df["基礎ポイント"] = df["順位"].apply(get_base_point)
                df["獲得ポイント"] = df["基礎ポイント"] * participants

                monthly_key = datetime.now().strftime("%Y-%m")

                sheet = get_sheet()
                records = sheet.get_all_records()
                existing_df = pd.DataFrame(records)

                # 今月データのみ抽出
                if not existing_df.empty:
                    existing_df = existing_df[existing_df["month"] == monthly_key]
                else:
                    existing_df = pd.DataFrame(columns=["month", "id", "name", "points"])

                # 新規データ整形
                new_data = []
                for _, row in df.iterrows():
                    new_data.append({
                        "month": monthly_key,
                        "id": str(row["識別番号"]),
                        "name": row["氏名"],
                        "points": int(row["獲得ポイント"])
                    })

                new_df = pd.DataFrame(new_data)

                combined = pd.concat([existing_df, new_df])

                # 識別番号で合算
                grouped = combined.groupby(["month", "id"]).agg({
                    "name": "last",
                    "points": "sum"
                }).reset_index()

                # シート全消去→再書き込み
                sheet.clear()
                sheet.append_row(["month", "id", "name", "points"])
                for _, row in grouped.iterrows():
                    sheet.append_row([
                        row["month"],
                        row["id"],
                        row["name"],
                        row["points"]
                    ])

                # ランキング作成（全員）
                ranking = grouped.sort_values(
                    by="points", ascending=False
                )

                result = f"🏆 {monthly_key} マンスリーランキング\n\n"
                for i, row in enumerate(ranking.itertuples(), 1):
                    result += f"{i}位 {row.name} - {row.points}pt\n"

                await message.channel.send(result)

bot.run(TOKEN)
