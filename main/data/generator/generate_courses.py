#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sinh dữ liệu: mon_hoc (courses.csv) + lop_hoc_phan (sections.csv) +
lop_hoc_phan_counter (sections_counter.csv) cho db-service TOPSIS-Hybrid.

Mô hình môn học/lớp học phỏng theo files(2)/main.go (thí nghiệm):
  - numCourses môn, mỗi môn sectionsPerCourse lớp (mặc định 3).
  - Tổng sức chứa tính NGƯỢC từ overloadRatio = nhu cầu / sức chứa:
        totalCapacity = numStudents / overloadRatio
        capPerSection  = totalCapacity / (numCourses * sectionsPerCourse)
  - Cấu trúc sức chứa này đảm bảo khi sinh numStudents nguyện vọng
    (generate_students.py) thì tổng nhu cầu đúng bằng overloadRatio lần
    tổng sức chứa -> thí nghiệm quá tải có ý nghĩa.

thoi_khoa_bieu định dạng: "T2|07:00-09:00;T4|07:00-09:00"
  (buổi = <ngày>|<giờ bắt đầu>-<giờ kết thúc>, nhiều buổi cách nhau ';')
  -- đúng định dạng mà countScheduleConflicts() trong db.go parse được.

Cách dùng:
    python3 generate_courses.py [num_courses] [num_students] [overload_ratio] [seed]
    ví dụ: python3 generate_courses.py 60 5000 2.0 42
"""
import csv
import random
import sys

NUM_COURSES = int(sys.argv[1]) if len(sys.argv) > 1 else 60
NUM_STUDENTS = int(sys.argv[2]) if len(sys.argv) > 2 else 5000
OVERLOAD_RATIO = float(sys.argv[3]) if len(sys.argv) > 3 else 2.0
SEED = int(sys.argv[4]) if len(sys.argv) > 4 else 42
SECTIONS_PER_COURSE = 3

random.seed(SEED)

COURSE_NAMES = [
    "Toan Cao Cap", "Giai Tich 1", "Giai Tich 2", "Dai So Tuyen Tinh", "Xac Suat Thong Ke",
    "Phuong Trinh Vi Phan", "Tri Tue Nhan Tao", "Hoc May", "Khai Pha Du Lieu", "Co So Du Lieu",
    "He Quan Tri Co So Du Lieu", "Lap Trinh Python", "Lap Trinh Java", "Lap Trinh C",
    "Cau Truc Du Lieu & Giai Thuat", "He Dieu Hanh", "Mang May Tinh", "An Toan Thong Tin",
    "Ky Thuat Phan Mem", "Cong Nghe Web", "Dien Toan Dam May", "Xu Ly Anh So", "Ngon Ngu Hinh Thuc",
    "Kien Truc May Tinh", "Lap Trnh Di Dong", "Kiem Thu Phan Mem", "Dong Hoa Quy Trinh",
    "Ke Toan Tai Chinh", "Quan Tri Kinh Doanh", "Marketing Can Ban", "Kinh Te Vi Mo",
    "Kinh Te Vi Mo", "To Chuc Va Quan Ly", "Vat Ly Dai Cuong", "Hoa Dai Cuong", "Sinh Hoc Dai Cuong",
    "Tieng Anh Chuyen Nganh", "Phap Luat Dai Cuong", "Triet Hoc Mac-Lenin", "Tu Tuong Ho Chi Minh",
    "Giao Duc Quoc Phong", "The Duc", "Tin Hoc Dai Cuong", "Thiet Ke Web", "Do Hoa May Tinh",
    "Bien Dịch", "Xu Ly Ngôn Ngữ Tự Nhiên", "Thị Giác Máy Tính", "Mô Phỏng Hệ Thống", "Tối Ưu Hóa",
    "Mạng Nơ-Ron", "Hệ Thống Nhúng", "Kiến Trúc Phần Mềm", "Quản Lý Dự Án", "Phân Tích Yêu Cầu",
    "Thiết Kế Cơ Sở Dữ Liệu", "Đồ Án Tốt Nghiệp", "Chuyên Đề Nâng Cao", "Hội Thảo Khoa Học", "Khoa Học Dữ Liệu Y Sinh",
]

# Các buổi học khả dụng: (ngày, giờ bắt đầu, giờ kết thúc)
SLOTS = [
    ("T2", "07:00", "09:00"), ("T2", "09:30", "11:30"), ("T2", "13:30", "15:30"),
    ("T3", "07:00", "09:00"), ("T3", "09:30", "11:30"), ("T3", "13:30", "15:30"),
    ("T4", "07:00", "09:00"), ("T4", "09:30", "11:30"), ("T4", "13:30", "15:30"),
    ("T5", "07:00", "09:00"), ("T5", "09:30", "11:30"), ("T5", "13:30", "15:30"),
    ("T6", "07:00", "09:00"), ("T6", "09:30", "11:30"), ("T6", "13:30", "15:30"),
    ("T7", "07:00", "09:00"), ("T7", "09:30", "11:30"),
]

def format_schedule(slots):
    """Chuyển danh sách (ngay, start, end) -> "T2|07:00-09:00;T4|07:00-09:00"."""
    return ";".join(f"{d}|{s}-{e}" for d, s, e in slots)

def schedule_slots():
    """1-2 buổi rời nhau, không lặp ngày trong cùng một lịch."""
    n = random.randint(1, 2)
    pool = SLOTS[:]
    random.shuffle(pool)
    picked, used_days = [], set()
    for item in pool:
        if len(picked) >= n:
            break
        if item[0] in used_days:
            continue
        picked.append(item)
        used_days.add(item[0])
    return sorted(picked, key=lambda x: SLOTS.index(x))

# ---- Tính sức chứa ngược từ overload ratio (giống generateData trong main.go) ----
total_sections = NUM_COURSES * SECTIONS_PER_COURSE
total_capacity = int(NUM_STUDENTS / OVERLOAD_RATIO)
if total_capacity < total_sections:
    total_capacity = total_sections  # đảm bảo mỗi lớp có ít nhất 1 chỗ
cap_per_section = total_capacity // total_sections

with open('courses.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow([
        'ma_mon_hoc', 'ten_mon_hoc', 'so_tin_chi', 'don_gia', 'trang_thai',
        'created_at', 'updated_at',
    ])
    for i in range(NUM_COURSES):
        cid = f'MH{i:03d}'
        name = COURSE_NAMES[i % len(COURSE_NAMES)]
        writer.writerow([cid, name, random.choice([2, 3, 4]), random.randint(800000, 1500000) / 1000,
                         'Mo dang ky', '2026-08-01 00:00:00', '2026-08-01 00:00:00'])

with open('sections.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow([
        'ma_lop_hoc_phan', 'ma_mon_hoc', 'ten_lop_hoc_phan', 'ma_sinh_vien',
        'phong_hoc', 'thoi_khoa_bieu', 'so_luong_toi_da', 'trang_thai',
        'ngay_bat_dau', 'ngay_ket_thuc', 'created_at', 'updated_at',
    ])
    for c in range(NUM_COURSES):
        cid = f'MH{c:03d}'
        for s in range(SECTIONS_PER_COURSE):
            sec_id = f'{cid}-L{s}'
            teacher = f'GV{c:03d}'
            room = f'R{100 + ((c * SECTIONS_PER_COURSE + s) % 150)}'
            tkb = format_schedule(schedule_slots())
            writer.writerow([
                sec_id, cid, f'{cid} - Lop {s + 1}', teacher, room, tkb, cap_per_section,
                'Mo dang ky', '2026-08-20 00:00:00', '2026-08-30 23:59:59',
                '2026-08-01 00:00:00', '2026-08-01 00:00:00',
            ])

with open('sections_counter.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['ma_lop_hoc_phan', 'so_luong_da_dang_ky'])
    for c in range(NUM_COURSES):
        for s in range(SECTIONS_PER_COURSE):
            writer.writerow([f'MH{c:03d}-L{s}', 0])

print(f"Generated courses.csv, sections.csv, sections_counter.csv "
      f"(courses={NUM_COURSES}, sections={total_sections}, cap/section={cap_per_section}, "
      f"overload={OVERLOAD_RATIO}x)")