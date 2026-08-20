#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sinh dữ liệu sinh viên + hồ sơ học vụ (sinh_vien_ho_so) + nguyện vọng đăng ký
cho db-service TOPSIS-Hybrid.

Phỏng theo đúng bộ sinh dữ liệu của thí nghiệm files(2)/main.go:
  - Dùng kỹ thuật FOURIER (tổng hàm sin theo vị trí cohort t=i/N thay vì
    random rời rạc từng người) để tạo:
      * mật độ nộp đơn không đều theo thời gian (đợt cao điểm mở đăng ký /
        nhắc nhở giữa kỳ / sát hạn) -> ảnh hưởng trực tiếp tới FIFO;
      * các thuộc tính tương quan mượt theo nhóm sinh viên.
  - Nhóm "cấp thiết cao" (urgent) chiếm ~15%, định nghĩa bởi các tiêu chí
    nguy_co_cham_tot_nghiep / so_ky_da_cho / so_lan_dang_ky_that_bai.
  - Mỗi nguyện vọng khai 1 lớp CHÍNH + 1 lớp PHỤ (lớp thay thế cùng môn) --
    chính là dữ liệu để chạy phân bổ HYBRID (AllocationOptimizer).

Cách dùng (chạy generate_courses.py TRƯỚC để có sections.csv):
    python3 generate_students.py [num_students] [seed]
    ví dụ: python3 generate_students.py 5000 42
"""
import csv
import math
import random
import sys
from datetime import datetime, timedelta

NUM_STUDENTS = int(sys.argv[1]) if len(sys.argv) > 1 else 5000
SEED = int(sys.argv[2]) if len(sys.argv) > 2 else 42

random.seed(SEED)

# ---- Đọc sections.csv: course -> danh sách lớp (id, ten, phong, tkb, siso) ----
course_sections = {}
section_info = {}
with open('sections.csv', newline='') as f:
    reader = csv.DictReader(f)
    for row in reader:
        cid = row['ma_mon_hoc']
        course_sections.setdefault(cid, []).append(row['ma_lop_hoc_phan'])
        section_info[row['ma_lop_hoc_phan']] = row
courses = sorted(course_sections.keys())

# ---- KỸ THUẬT FOURIER (giữ nguyên tinh thần thí nghiệm) ----

def fourier_intensity(t, components):
    v = 1.0
    for freq, amp, phase in components:
        v += amp * math.sin(2 * math.pi * freq * t + phase)
    return max(v, 0.05)

SUBMISSION_WAVES = [
    (1, 0.6, -math.pi / 2),
    (3, 0.3, math.pi),
    (6, 0.45, -math.pi / 2),
]

def sample_submission_times(n):
    """Lấy mẫu n thời điểm nộp đơn t in [0,1] theo mật độ Fourier (rejection)."""
    max_i = max(fourier_intensity(i / 1000.0, SUBMISSION_WAVES) for i in range(1001))
    times = []
    while len(times) < n:
        t = random.random()
        y = random.random() * max_i
        if y <= fourier_intensity(t, SUBMISSION_WAVES):
            times.append(t)
    return times

def fourier_score(t, components, lo, hi, noise_sigma):
    """Giá trị nguyên trong [lo,hi]: xu hướng Fourier mượt + nhiễu Gaussian nhỏ."""
    v = 1.0
    for freq, amp, phase in components:
        v += amp * math.sin(2 * math.pi * freq * t + phase)
    norm = max(0.0, min(1.0, v / 2.0))
    val = lo + norm * (hi - lo) + random.gauss(0, noise_sigma)
    return max(lo, min(hi, int(round(val))))

# Các chuỗi Fourier riêng cho từng tiêu chí (tần số/pha khác nhau để các cột
# không dao động đồng bộ, tránh tương quan giả tạo làm sai lệch trọng số EWM).
MANDATORY_WAVE = [(2, 0.5, 0), (5, 0.25, math.pi / 3)]
CREDITS_WAVE = [(1, 0.7, -math.pi / 2), (4, 0.2, math.pi)]
LOAD_WAVE = [(3, 0.4, math.pi / 4)]
PREF_MATCH_WAVE = [(2, 0.5, math.pi)]
CAN_OPEN_WAVE = [(2, 0.45, -math.pi / 3)]

# ---- Dải thời gian đăng ký ----
REG_START = datetime(2026, 8, 20, 0, 0, 0)
REG_WINDOW = timedelta(days=5)

last_names = ["Nguyen", "Tran", "Le", "Pham", "Hoang", "Phan", "Vu", "Dang", "Bui", "Do"]
mid_names = ["Van", "Thi", "Quoc", "Minh", "Duc", "Thu", "Anh", "Hong"]
first_names = ["Huy", "Nam", "Long", "An", "Binh", "Chau", "Dung", "Giang", "Hoa", "Khoi",
               "Lan", "Mai", "Ngan", "Oanh", "Phuc", "Quan", "Sang", "Tam", "Trang", "Vy"]

hoho_first = random.choice(last_names)
hoho_mid = random.choice(mid_names)

# ---- SINH SINH VIÊN ----
submission_times = sample_submission_times(NUM_STUDENTS)

with open('students.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['ma_sinh_vien', 'ho', 'ten', 'gioi_tinh', 'email',
                     'so_dien_thoai', 'ma_lop', 'created_at', 'updated_at'])
    for i in range(NUM_STUDENTS):
        sid = f'SV{i:06d}'
        ho = f"{random.choice(last_names)} {random.choice(mid_names)}"
        ten = random.choice(first_names)
        writer.writerow([sid, ho, ten, random.choice(['Nam', 'Nu']),
                         f'{sid.lower()}@university.edu.vn',
                         f'09{random.randint(20000000, 99999999)}',
                         f'L{random.randint(1, 5)}',
                         '2026-08-01 00:00:00', '2026-08-01 00:00:00'])

# ---- SINH HỒ SƠ HỌC VỤ (sinh_vien_ho_so) + NGUYỆN VỌNG (dang_ky) ----
with open('student_profiles.csv', 'w', newline='') as f_profile:
    writer_profile = csv.writer(f_profile)
    writer_profile.writerow([
        'ma_sinh_vien', 'uu_tien_bat_buoc', 'nguy_co_cham_tot_nghiep', 'so_ky_da_cho',
        'so_lan_dang_ky_that_bai', 'so_tin_chi_tich_luy', 'khoi_luong_hoc_ky_hien_tai',
        'muc_phu_hop_nguyen_vong', 'so_lop_thay_the', 'kha_nang_mo_them_lop', 'updated_at',
    ])

    with open('registrations.csv', 'w', newline='') as f_reg:
        writer_reg = csv.writer(f_reg)
        writer_reg.writerow([
            'ma_dang_ky', 'ma_sinh_vien', 'ho', 'ten', 'ma_lop_hoc_phan',
            'ten_lop_hoc_phan', 'ma_mon_hoc', 'phong_hoc', 'thoi_khoa_bieu',
            'so_luong_toi_da', 'hinh_thuc', 'ngay_dang_ky', 'trang_thai',
            'ma_lop_hoc_phan_phu', 'ten_lop_hoc_phan_phu', 'phong_hoc_phu',
            'thoi_khoa_bieu_phu', 'so_luong_toi_da_phu', 'created_at', 'updated_at',
        ])

        for i in range(NUM_STUDENTS):
            sid = f'SV{i:06d}'
            t = i / NUM_STUDENTS  # vị trí cohort

            urgent = random.random() < 0.15
            mandatory = fourier_score(t, MANDATORY_WAVE, 1, 5, 0.4)
            if urgent:
                grad_risk = random.randint(4, 5)
                sem_waited = random.randint(2, 5)
                failed = random.randint(1, 3)
            else:
                grad_risk = random.randint(1, 3)
                sem_waited = random.randint(0, 1)
                failed = 0
            credits = fourier_score(t, CREDITS_WAVE, 0, 140, 8)
            load = fourier_score(t, LOAD_WAVE, 10, 22, 1.2)
            pref_match = fourier_score(t, PREF_MATCH_WAVE, 1, 5, 0.4)
            can_open = fourier_score(t, CAN_OPEN_WAVE, 1, 5, 0.4)

            # Chọn 1 môn học + lớp chính + lớp phụ (lớp khác CÙNG môn)
            cid = random.choice(courses)
            secs = course_sections[cid]
            primary = random.choice(secs)
            alt = random.choice([s for s in secs if s != primary]) if len(secs) > 1 else ""
            # C9: số lớp thay thế THỰC TẾ của môn học (giá trị chi phí trong TOPSIS)
            alt_count = len(secs) - 1 if len(secs) > 1 else 0

            writer_profile.writerow([
                sid, mandatory, grad_risk, sem_waited, failed, credits, load,
                pref_match, alt_count, can_open, '2026-08-01 00:00:00',
            ])

            # Thời điểm nộp đơn theo mật độ Fourier -> ngày giờ đăng ký
            rtime = REG_START + submission_times[i] * REG_WINDOW
            ts = rtime.strftime('%Y-%m-%d %H:%M:%S')
            ho = f"{random.choice(last_names)} {random.choice(mid_names)}"
            ten = random.choice(first_names)

            p = section_info[primary]
            if alt:
                a = section_info[alt]
                writer_reg.writerow([
                    f'DK_{sid}_{primary}_{i}', sid, ho, ten, primary,
                    p['ten_lop_hoc_phan'], cid, p['phong_hoc'], p['thoi_khoa_bieu'],
                    p['so_luong_toi_da'], 'Chinh quy', ts, 'ChoXuLy',
                    alt, a['ten_lop_hoc_phan'], a['phong_hoc'], a['thoi_khoa_bieu'],
                    a['so_luong_toi_da'], ts, ts,
                ])
            else:
                writer_reg.writerow([
                    f'DK_{sid}_{primary}_{i}', sid, ho, ten, primary,
                    p['ten_lop_hoc_phan'], cid, p['phong_hoc'], p['thoi_khoa_bieu'],
                    p['so_luong_toi_da'], 'Chinh quy', ts, 'ChoXuLy',
                    '', '', '', '', '', ts, ts,
                ])

print(f"Generated students.csv, student_profiles.csv, registrations.csv "
      f"(students={NUM_STUDENTS}, urgent~15%, registrations={NUM_STUDENTS})")