#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage: $0 [--repo PATH] [--max N] [--allow-empty] [--push] [--dry-run]

Options:
  --repo PATH     Đường dẫn đến repo git (mặc định: thư mục hiện tại)
  --max N         Số lượng commit tối đa muốn tạo (mặc định: 10000)
  --allow-empty   Nếu bật, tạo commit trống cho đủ số lượng MAX khi hết file thay đổi
  --push          Đẩy thẳng lên nhánh hiện tại (ví dụ: main) sau khi commit xong
  --dry-run       In ra các hành động thử nghiệm, không commit thật
  --help          Hiển thị trợ giúp này
EOF
}

REPO="."
MAX_COMMITS=10000
ALLOW_EMPTY=false
DO_PUSH=false
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo)
      REPO="$2"; shift 2;;
    --max)
      MAX_COMMITS="$2"; shift 2;;
    --allow-empty)
      ALLOW_EMPTY=true; shift;;
    --push)
      DO_PUSH=true; shift;;
    --dry-run)
      DRY_RUN=true; shift;;
    --help)
      usage; exit 0;;
    *)
      echo "Lựa chọn không hợp lệ: $1"; usage; exit 1;;
  esac
done

if [[ ! -d "$REPO" ]]; then
  echo "Không tìm thấy đường dẫn Repo: $REPO" >&2
  exit 1
fi

pushd "$REPO" >/dev/null
GIT_ROOT=$(git rev-parse --show-toplevel 2>/dev/null || true)
if [[ -z "$GIT_ROOT" ]]; then
  echo "Thư mục này không phải Git repository: $REPO" >&2
  popd >/dev/null
  exit 1
fi

# Lấy tên nhánh hiện tại đang đứng (ví dụ: main, master...)
CURRENT_BRANCH=$(git branch --show-current)
if [[ -z "$CURRENT_BRANCH" ]]; then
  echo "Lỗi: Không tìm thấy tên nhánh hiện tại (Có thể đang ở trạng thái detached HEAD)." >&2
  popd >/dev/null
  exit 1
fi

echo "Repository root: $GIT_ROOT"
echo "Nhánh hiện tại: $CURRENT_BRANCH"
echo "Số commit tối đa: $MAX_COMMITS"
echo "Cho phép commit trống: $ALLOW_EMPTY"
echo "Chạy thử (Dry run): $DRY_RUN"

# Tìm các file thay đổi (modified + untracked, loại trừ file bị ignore)
mapfile -d '' files < <(git ls-files -m -o --exclude-standard -z || true)
NUM_FILES=${#files[@]}
echo "Tìm thấy $NUM_FILES file có thay đổi."

if [[ $NUM_FILES -eq 0 && "$ALLOW_EMPTY" != "true" ]]; then
  echo "Không có file nào thay đổi để commit. Thoát." >&2
  popd >/dev/null
  exit 1
fi

if [[ "$DRY_RUN" == "true" ]]; then
  echo "DRY RUN: Sẽ commit thẳng vào nhánh: $CURRENT_BRANCH"
  echo "DRY RUN: Danh sách file thay đổi (tối đa 20 file đầu):"
  for i in "${!files[@]}"; do
    [[ $i -ge 20 ]] && break
    printf '%s\n' "${files[$i]}"
  done
  popd >/dev/null
  exit 0
fi

COUNT=0
TARGET=$MAX_COMMITS

# Vòng lặp commit từng file một
for f in "${files[@]}"; do
  if [[ $COUNT -ge $TARGET ]]; then
    break
  fi
  ((COUNT++))
  echo "[$COUNT/$TARGET] Đang commit file: $f"
  git add -- "$f"
  git commit -m "chore: commit file $f" --quiet || {
    echo "Lỗi: Commit thất bại cho file $f" >&2
    popd >/dev/null
    exit 1
  }
done

# Tạo thêm commit trống nếu có bật --allow-empty
if [[ $COUNT -lt $TARGET ]]; then
  REMAIN=$((TARGET-COUNT))
  if [[ "$ALLOW_EMPTY" == "true" ]]; then
    echo "Tạo thêm $REMAIN commit trống để đạt mục tiêu $TARGET commits"
    for ((i=1;i<=REMAIN;i++)); do
      ((COUNT++))
      echo "[$COUNT/$TARGET] Commit trống #$i"
      git commit --allow-empty -m "chore: empty commit $COUNT" --quiet || {
        echo "Lỗi: Commit trống thất bại tại số #$i" >&2
        popd >/dev/null
        exit 1
      }
    done
  else
    echo "Chỉ tạo được $COUNT commits do chỉ có $NUM_FILES file thay đổi. Dùng thêm --allow-empty nếu muốn tạo đủ $TARGET commits." >&2
  fi
fi

echo "Đã commit xong toàn bộ vào nhánh $CURRENT_BRANCH. Tổng số commit: $COUNT"

# Thực hiện Push thẳng lên origin của nhánh hiện tại
if [[ "$DO_PUSH" == "true" ]]; then
  echo "Đang push thẳng lên origin $CURRENT_BRANCH..."
  git push origin "$CURRENT_BRANCH"
  echo "Đã đẩy code lên github/gitlab thành công!"
else
  echo "Bỏ qua bước push (sử dụng tham số --push để tự động push)."
fi

popd >/dev/null
exit 0
