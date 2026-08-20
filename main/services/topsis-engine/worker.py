import asyncio
import nats
import json
import numpy as np

# Trọng số mẫu (sẽ lấy từ biến môi trường hoặc DB)
WEIGHTS = {
    "mandatory": 0.2,
    "graduation_risk": 0.25,
    "waiting_semesters": 0.2,
    "alternative_classes": 0.15,
    "schedule_conflict": 0.1,
    "time_submitted": 0.1
}
CRITERIA_BENEFIT = ["mandatory", "graduation_risk", "waiting_semesters", "time_submitted"]
CRITERIA_COST = ["alternative_classes", "schedule_conflict"]

def topsis_scores(students):
    # students: list of dict with criteria values
    matrix = np.array([[s[c] for c in CRITERIA_BENEFIT + CRITERIA_COST] for s in students])
    # Chuẩn hóa vector
    norm = np.sqrt((matrix**2).sum(axis=0))
    matrix_norm = matrix / norm
    # Gán trọng số
    weights_arr = np.array([WEIGHTS[c] for c in CRITERIA_BENEFIT + CRITERIA_COST])
    weighted = matrix_norm * weights_arr
    # Xác định ideal best/worst
    n_benefit = len(CRITERIA_BENEFIT)
    ideal_best = np.zeros(weighted.shape[1])
    ideal_worst = np.zeros(weighted.shape[1])
    for j in range(weighted.shape[1]):
        if j < n_benefit:
            ideal_best[j] = np.max(weighted[:, j])
            ideal_worst[j] = np.min(weighted[:, j])
        else:
            ideal_best[j] = np.min(weighted[:, j])
            ideal_worst[j] = np.max(weighted[:, j])
    # Tính khoảng cách
    s_best = np.sqrt(((weighted - ideal_best)**2).sum(axis=1))
    s_worst = np.sqrt(((weighted - ideal_worst)**2).sum(axis=1))
    scores = s_worst / (s_best + s_worst + 1e-9)
    return scores.tolist()

async def run():
    nc = await nats.connect("nats://nats:4222")
    js = nc.jetstream()
    sub = await js.subscribe("eligible.requests", durable="topsis")
    async for msg in sub.messages:
        data = json.loads(msg.data)
        # data['students'] là danh sách sinh viên trong một lô? Hoặc xử lý từng yêu cầu riêng lẻ.
        # Ở đây giả định mỗi message chứa một batch sinh viên đủ điều kiện cho một lớp.
        students = data['students']  # list of dict
        scores = topsis_scores(students)
        # Gán điểm cho từng student
        for i, student in enumerate(students):
            student['topsis_score'] = scores[i]
        await js.publish("scored.requests", json.dumps(data).encode())
        await msg.ack()
    await nc.close()

if __name__ == '__main__':
    asyncio.run(run())