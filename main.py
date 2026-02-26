import discord
import pandas as pd
import io
import os
import json
from datetime import datetime

TOKEN = os.getenv("TOKEN")

intents = discord.Intents.default()
intents.message_content = True
bot = discord.Client(intents=intents)

DATA_FILE = "monthly_data.json"

def load_data():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_data(data):
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

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
                data = load_data()

                if monthly_key not in data:
                    data[monthly_key] = {}

                for _, row in df.iterrows():
                    player_id = str(row["識別番号"])
                    name = row["氏名"]
                    points = int(row["獲得ポイント"])

                    if player_id not in data[monthly_key]:
                        data[monthly_key][player_id] = {
                            "name": name,
                            "points": 0
                        }

                    data[monthly_key][player_id]["name"] = name
                    data[monthly_key][player_id]["points"] += points

                save_data(data)

                ranking = sorted(
                    data[monthly_key].values(),
                    key=lambda x: x["points"],
                    reverse=True
                )

                result = f"🏆 {monthly_key} マンスリーランキング\n\n"
                for i, player in enumerate(ranking[:10], 1):
                    result += f"{i}位 {player['name']} - {player['points']}pt\n"

                await message.channel.send(result)

bot.run(TOKEN)
