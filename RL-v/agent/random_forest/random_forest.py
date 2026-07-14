import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
import redis
import json
import time
import random
import os

df = pd.read_csv("./dataset/cloud_dataset.csv")
df.drop(["Timestamp", "User_ID", "Workload_Type"], axis=1, inplace=True)

print(df.head())

X = df.drop(columns=["Anomaly_Label"])
y = df.get("Anomaly_Label")

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)
FEATURE_COLS = X_train.columns.tolist()

print("X_train:", len(X_train))
print("X_test:", len(X_test))
print("y_train:", len(y_train))
print("y_test:", len(y_test))

clf = RandomForestClassifier(max_depth=2, random_state=0)
clf.fit(X_train, y_train)
 

w = redis.Redis(
        host="192.168.24.2", port=6379, db=0, decode_responses=True)
 

def RandomForestClassifier_Worker():
    while True:
        DRAGONFLY_NODES = ["192.168.24.4", "192.168.24.3"]
        df_host = random.choice(DRAGONFLY_NODES)
        r = redis.Redis(
        host=df_host, port=6379, db=0, decode_responses=True)
        rows = []
        # lấy tất cả key NODE-*
        keys = r.keys("NODE-*")
        for key in keys:
            try: 
                data = r.execute_command("JSON.GET", key)
                if data:
                    rows.append(json.loads(data))
            except Exception as e:
                 print("Error fetching key:", e)
  
        if not rows:
            print("No NODE data found in Dragonfly. Retrying...")
            time.sleep(5)
            continue

        X_test_live = pd.DataFrame(rows)
        node_ids = X_test_live.get("NODE_ID")

        X_predict = X_test_live.drop(columns=["NODE_ID"])
        # Keep feature order consistent with training
        X_predict = X_predict.reindex(columns=FEATURE_COLS).fillna(0)
        
        # print(X_predict.head())

        pred = clf.predict(X_predict)
        anomaly_nodes = [node_ids.iloc[i] for i, p in enumerate(pred) if p == 0]

        # FIX BUG K8S PLUGIN CRASH: Chỉ lưu ID (số) vào key random_forest_TOP
        if len(anomaly_nodes) == 0:
            random_index = random.randrange(len(X_test_live))
            top_node_id = int(X_test_live.iloc[random_index]["NODE_ID"])
            r.set("random_forest_TOP", str(top_node_id))
            print(f"No anomaly. Set random_forest_TOP to: {top_node_id}")
            
        elif len(anomaly_nodes) > 0:
            random_index = random.randrange(len(anomaly_nodes))
            top_node_id = int(anomaly_nodes[random_index])
            w.set("random_forest_TOP", str(top_node_id))
            print(f"Anomaly detected! Set random_forest_TOP to: {top_node_id}")

        time.sleep(5)

RandomForestClassifier_Worker()