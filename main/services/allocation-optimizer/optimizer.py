import time
import redis
import json
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from ortools.sat.python import cp_model

def main():
    # Kết nối ScyllaDB
    cluster = Cluster(['scylla-client.course-reg-exp.svc.cluster.local'])
    session = cluster.connect('registration')
    session.row_factory = dict_factory

    # Lấy danh sách sinh viên đã có điểm TOPSIS và lớp học
    rows_students = session.execute("SELECT student_id, topsis_score, preferences FROM scored_students")
    rows_sections = session.execute("SELECT section_id, capacity FROM sections")

    students = list(rows_students)
    sections = list(rows_sections)
    capacities = {s['section_id']: s['capacity'] for s in sections}

    # Xây dựng mô hình CP-SAT (đơn giản)
    model = cp_model.CpModel()
    x = {}
    for i, stu in enumerate(students):
        for pref in stu['preferences']:
            if pref in capacities:
                x[(i, pref)] = model.NewBoolVar(f'x_{i}_{pref}')

    # Ràng buộc: mỗi sinh viên tối đa 1 lớp (cho mỗi môn học – giả định preferences cùng môn)
    for i, stu in enumerate(students):
        model.Add(sum(x[(i, p)] for p in stu['preferences'] if (i, p) in x) <= 1)

    # Ràng buộc sĩ số
    for sec in capacities:
        model.Add(sum(x[(i, sec)] for i, stu in enumerate(students) if (i, sec) in x) <= capacities[sec])

    # Hàm mục tiêu: tối đa tổng điểm TOPSIS
    objective = sum(x[(i, p)] * int(stu['topsis_score'] * 100) for i, stu in enumerate(students) for p in stu['preferences'] if (i, p) in x)
    model.Maximize(objective)

    solver = cp_model.CpSolver()
    status = solver.Solve(model)

    # Ghi kết quả vào ScyllaDB
    insert_stmt = session.prepare("INSERT INTO allocation_results (student_id, section_id, status) VALUES (?, ?, 'ALLOCATED')")
    if status == cp_model.OPTIMAL or status == cp_model.FEASIBLE:
        for i, stu in enumerate(students):
            allocated = False
            for p in stu['preferences']:
                if (i, p) in x and solver.Value(x[(i, p)]) == 1:
                    session.execute(insert_stmt, (stu['student_id'], p))
                    allocated = True
                    break
            if not allocated:
                # Đưa vào danh sách chờ (bảng waiting_list)
                pass
    print("Optimization completed.")

if __name__ == '__main__':
    main()