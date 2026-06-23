import asyncio
import websockets
import json
import os

# ================= 伺服器全域資料庫 =================
connected_clients = set()  # 紀錄所有連線中的玩家
waiting_players = []       # 快速配對佇列：存放字典 {"ws": websocket, "uid": uid, "name": name}
rooms = {}                 # 私人房間資料庫：字典 {"房間名稱/密碼": [websocket1, websocket2]}
battle_rooms = {}          # 戰場房間資料庫，負責實時對決同步

# === 廣播功能：更新大廳的「房間列表」 ===
async def broadcast_room_list():
    """將目前等待中（只有1人）的房間名稱廣播給所有連線的玩家"""
    if not connected_clients:
        return
        
    # 只挑選出「人數為 1」的房間
    available_rooms = [room_name for room_name, players in rooms.items() if len(players) == 1]
    
    msg = json.dumps({
        "action": "room_list_update",
        "rooms": available_rooms
    })
    
    # 群發給所有在線上的客戶端
    websockets.broadcast(connected_clients, msg)


# === 核心處理器：接收玩家的連線與訊息 ===
async def game_handler(websocket):
    global waiting_players  # 確保全域宣告在最頂端，避免引發 SyntaxError
    
    # 玩家連線時，加入全域清單並發送最新的房間列表
    connected_clients.add(websocket)
    await broadcast_room_list()
    
    try:
        async for message in websocket:
            data = json.loads(message)
            action = data.get("action")

            # ----------------------------------------------------
            # 模式一：隨機快速配對 (Quick Match)
            # ----------------------------------------------------
            if action == "match":
                player_uid = data.get("uid", "")
                player_name = data.get("name", "未知巫師")
                
                # 1. 防止連點：檢查是否已經在排隊佇列中
                if any(p["ws"] == websocket for p in waiting_players):
                    await websocket.send(json.dumps({"action": "error", "msg": "您已經在配對佇列中，請耐心等候！"}))
                    continue
                
                # 2. 尋找對手：遍歷排隊佇列，尋找「UID 與當前玩家不同」的人
                match_partner = None
                for p in waiting_players:
                    if p["uid"] != player_uid:
                        match_partner = p
                        break
                
                # 3. 結算配對
                if match_partner:
                    waiting_players.remove(match_partner) # 將對手移出佇列
                    
                    p1 = match_partner["ws"]
                    p2 = websocket
                    
                    # 生成唯一戰場房號
                    room_id = f"battle_{player_uid}_{match_partner['uid']}"
                    
                    # 傳送包含房號的配對成功訊息給雙方
                    await p1.send(json.dumps({"action": "match_success", "room_id": room_id}))
                    await p2.send(json.dumps({"action": "match_success", "room_id": room_id}))
                    print(f"⚔️ 快速配對成功，創立戰場：{room_id}")
                else:
                    # 如果沒找到符合的對手，就把自己放進排隊名單
                    waiting_players.append({
                        "ws": websocket, 
                        "uid": player_uid, 
                        "name": player_name
                    })
                    await websocket.send(json.dumps({"action": "waiting", "msg": "正在尋找其他線上玩家，請稍候..."}))
            
            # ----------------------------------------------------
            # 模式二：創建私人房間 (Create Room)
            # ----------------------------------------------------
            elif action == "create_room":
                room_pwd = data.get("password")
                if not room_pwd:
                    continue
                    
                if room_pwd in rooms:
                    await websocket.send(json.dumps({"action": "error", "msg": "這個房間名稱已經被使用了，請換一個！"}))
                else:
                    rooms[room_pwd] = [websocket]
                    await websocket.send(json.dumps({"action": "waiting", "msg": f"房間【{room_pwd}】創建成功！等待對手加入..."}))
                    print(f"🚪 新房間建立：{room_pwd}")
                    await broadcast_room_list() # 更新所有人的房間大廳

            # ----------------------------------------------------
            # 模式三：加入私人房間 (Join Room)
            # ----------------------------------------------------
            elif action == "join_room":
                room_pwd = data.get("password")
                
                if room_pwd in rooms and len(rooms[room_pwd]) == 1:
                    if rooms[room_pwd][0] == websocket:
                        await websocket.send(json.dumps({"action": "error", "msg": "您不能加入自己創建的房間！"}))
                        continue
                        
                    # 成功加入，湊齊兩人，開打！
                    p1 = rooms[room_pwd][0]
                    p2 = websocket
                    
                    # 生成私人專屬戰場房號
                    room_id = f"battle_{room_pwd}"
                    await p1.send(json.dumps({"action": "match_success", "room_id": room_id}))
                    await p2.send(json.dumps({"action": "match_success", "room_id": room_id}))
                    
                    print(f"⚔️ 房間【{room_pwd}】配對成功，戰鬥開始！")
                    
                    # 戰鬥開始後，將該房間從清單中移除
                    del rooms[room_pwd]
                    await broadcast_room_list()
                else:
                    await websocket.send(json.dumps({"action": "error", "msg": "找不到房號或房間已滿。"}))

            # ----------------------------------------------------
            # 模式四：戰場階段連線通訊與數據同步 (Battle Room Relay)
            # ----------------------------------------------------
            elif action == "join_battle":
                room_id = data.get("room_id")
                uid = data.get("uid")
                name = data.get("name")
                avatar = data.get("avatar")
                deck = data.get("deck", [])
                
                if room_id not in battle_rooms:
                    battle_rooms[room_id] = {}
                
                # 分配對戰位置 (玩家1 或 玩家2)
                if "p1" not in battle_rooms[room_id]:
                    battle_rooms[room_id]["p1"] = {"ws": websocket, "uid": uid, "name": name, "avatar": avatar, "deck": deck}
                    print(f"🎯 玩家1 【{name}】進入戰場房間: {room_id}")
                elif "p2" not in battle_rooms[room_id]:
                    battle_rooms[room_id]["p2"] = {"ws": websocket, "uid": uid, "name": name, "avatar": avatar, "deck": deck}
                    print(f"🎯 玩家2 【{name}】進入戰場房間: {room_id}")
                
                # 當雙方魔法師都就位，立即幫彼此交換手牌陣容資料，宣佈開戰
                if "p1" in battle_rooms[room_id] and "p2" in battle_rooms[room_id]:
                    p1 = battle_rooms[room_id]["p1"]
                    p2 = battle_rooms[room_id]["p2"]
                    
                    # 將玩家2的資料同步給玩家1
                    await p1["ws"].send(json.dumps({
                        "action": "battle_start", "opp_name": p2["name"], "opp_avatar": p2["avatar"], "opp_deck": p2["deck"]
                    }))
                    # 將玩家1的資料同步給玩家2
                    await p2["ws"].send(json.dumps({
                        "action": "battle_start", "opp_name": p1["name"], "opp_avatar": p1["avatar"], "opp_deck": p1["deck"]
                    }))
                    print(f"🔮 戰場數據對接完畢！【{p1['name']}】 VS 【{p2['name']}】正式開打！")

            # ----------------------------------------------------
            # 模式五：轉發戰鬥動作指令
            # ----------------------------------------------------
            elif action == "battle_action":
                room_id = data.get("room_id")
                if room_id in battle_rooms:
                    p1 = battle_rooms[room_id].get("p1")
                    p2 = battle_rooms[room_id].get("p2")
                    
                    # 判斷要把這份傷害指令轉發給誰
                    target_ws = None
                    if p1 and p1["ws"] == websocket: target_ws = p2["ws"] if p2 else None
                    elif p2 and p2["ws"] == websocket: target_ws = p1["ws"] if p1 else None
                    
                    if target_ws:
                        await target_ws.send(json.dumps({
                            "action": "opponent_action",
                            "attacker_idx": data.get("attacker_idx"),
                            "target_idx": data.get("target_idx"),
                            "target_type": data.get("target_type")
                        }))

    except websockets.exceptions.ConnectionClosed:
        # 玩家斷線時會自動觸發此區塊
        pass
        
    finally:
        # === 玩家斷線或離開網頁時的自動清理機制 ===
        if websocket in connected_clients:
            connected_clients.remove(websocket)
        
        # 1. 從快速配對佇列中移除
        waiting_players = [p for p in waiting_players if p["ws"] != websocket]
        
        # 2. 如果他有開房間但沒人加，把房間刪除
        empty_rooms = []
        for room_name, players in rooms.items():
            if websocket in players:
                players.remove(websocket)
            if len(players) == 0:
                empty_rooms.append(room_name)
                
        for room_name in empty_rooms:
            del rooms[room_name]
            print(f"🧹 房間【{room_name}】因房主離開已自動解散。")
            
        # 3. 移除沒人的對戰房間
        empty_battles = [r for r, p in battle_rooms.items() if (p.get("p1") and p["p1"]["ws"] == websocket) or (p.get("p2") and p["p2"]["ws"] == websocket)]
        for r in empty_battles:
            del battle_rooms[r]
            print(f"🧹 戰場房間【{r}】因玩家斷線已關閉結算。")
            
        # 4. 廣播更新房間清單
        await broadcast_room_list()

# === 啟動伺服器主程式 ===
async def main():
    # 讓伺服器自動讀取雲端分配的 Port，如果在本機跑就預設用 8765
    port = int(os.environ.get("PORT", 8765))
    
    # 將主機綁定修改為 "0.0.0.0"（代表接受外部所有 IP 的外部連線，此為雲端託管必須設定）
    async with websockets.serve(game_handler, "0.0.0.0", port):
        print("=========================================")
        print(f" 🚀 星宿魔法師雲端伺服器已成功啟動！")
        print(f" 📡 正在監聽 Port: {port}")
        print("=========================================")
        await asyncio.Future()  # 讓伺服器永久執行不中斷

if __name__ == "__main__":
    asyncio.run(main())