# Hướng dẫn nhập Codex vào CRS v3.0

> **Đọc trước khi nhập dữ liệu.** Tài liệu này dành cho người viết lore, rule, người biên tập và người vận hành indexer. Mã nguồn CRS nằm trong thư mục `crs/`. Tất cả thao tác đều chạy **local**, chỉ dùng Python standard library và SQLite. Không gửi dữ liệu lên database online.

## 1. Chuẩn bị nguồn

Mỗi Codex nên là một tệp Markdown UTF-8 có các heading rõ ràng. Tách section theo chủ đề hoặc thực thể trước khi nghĩ đến độ dài. Mỗi section chỉ nên mô tả một phạm vi nhất quán. Dùng nhãn loại chunk ngay dưới heading khi cần kiểm soát phân loại:

```markdown
# Kael
## Tiểu sử
<!-- CRS:FACT -->
Kael sinh tại Sài Gòn. Đây là một dữ kiện được xác nhận trong canon.

## Quy tắc bất tử
<!-- CRS:RULE -->
Nếu linh hồn bị tiêu diệt thì không thể hồi sinh.
Ngoại lệ: cơ chế đã được canon xác nhận có thể khôi phục linh hồn.

## Truyền thuyết
<!-- CRS:LORE -->
Đoạn lore có ngữ cảnh và liên kết nhân quả...

## Một cuộc chạm trán
<!-- CRS:NARRATIVE -->
Đoạn diễn biến truyện có trình tự...
```

Bốn nhãn hợp lệ là `FACT`, `RULE`, `LORE`, `NARRATIVE`. Khi không gắn nhãn, indexer sẽ dùng heuristic. **Luôn biên tập lại các RULE quan trọng** vì heuristic không thay thế được quyết định canon.

Mục tiêu token từ vựng: FACT 50–150, RULE 100–250, LORE 200–400, NARRATIVE 300–500. Token được đếm theo thuật toán lexical nội bộ, **không phải** tokenizer của một mô hình AI cụ thể. Một đơn vị ngữ nghĩa có thể ngắn hơn mức tối thiểu. Không tách điều kiện và ngoại lệ của RULE thành hai chunk; RULE nguyên tử quá dài sẽ bị từ chối để biên tập thủ công.

## 2. Chạy ingest và publish

Chạy từ thư mục gốc repository, với Python 3.10+:

```python
from pathlib import Path
from crs.ingest import ingest_file

report = ingest_file(
    "./codex-data",
    "KAE",
    "Kael",
    "./Kael.md",
    "R001",
)
print(report)
```

`ingest_file` thực hiện ingest rồi publish. Nếu cần tách hai giai đoạn để kiểm tra trước khi kích hoạt:

```python
from crs.ingest import OfflineIndexer, publish_atomic

source = open("Kael.md", encoding="utf-8").read()
indexer = OfflineIndexer("./codex-data")
try:
    build_report = indexer.ingest("KAE", "Kael", source, "R001")
    print(build_report)
finally:
    indexer.close()

release_report = publish_atomic("./codex-data", "R001")
print(release_report)
```

**Quy ước:** `KAE` là ví dụ về mã CDX, không được tự gán nếu đã có registry đang dùng. Không dùng lại CDX, section number hoặc chunk code đã được cấp. `R001` là ví dụ release ID, mỗi release mới cần ID mới. Nếu thất bại khi ingest, những mã đã cấp vẫn là tombstone và không được tái sử dụng.

## 3. Luồng phát hành và cách xử lý lỗi

1. **BUILD:** shard mới chỉ được ghi trong `staging/`.
2. **VERIFY:** kiểm tra schema, SQLite integrity, foreign key, checksum, SHA-256 nội dung, title index, anchor và kích thước shard.
3. **PREPARE:** chỉ artifact đã xác minh mới được chuyển sang `shards/`; tạo snapshot registry và manifest có hash.
4. **COMMIT:** thay thế atomically tệp con trỏ `CURRENT`.
5. **RETAIN:** giữ release cũ và shard bất biến để các session đã pin vẫn đọc được.

Không chỉnh sửa, UPDATE, DELETE hoặc REPLACE trực tiếp chunk đã publish. Nếu lore/rule đổi, ingest chunk mới vào release mới; giữ quan hệ lịch sử thay vì sửa nội dung đã xuất bản. Không tự xóa `staging/`, `shards/`, `releases/` hoặc `registry.db` để chữa lỗi nếu chưa kiểm tra trạng thái release. Lưu dữ liệu trên filesystem local; sao lưu toàn bộ data root trước thao tác quản trị có rủi ro.

## 4. GET theo intent đã pin

```python
from crs.store import CodexReader

reader = CodexReader("./codex-data", "R001")
try:
    reader.create_session("session-01")
    reader.pin_intent(
        "session-01",
        "intent-01",
        "KAE.003.00000X",  # thay bằng anchor THẬT do indexer cấp
        "KAE",
        3,
        "FACT",
        "Tiểu sử",
    )
    result = reader.get(
        "KAE.003.00174Z",  # thay bằng code THẬT được trả từ index
        "intent-01",
        "session-01",
    )
    print(result["status"])
finally:
    reader.close()
```

**Lưu ý:** các mã trong ví dụ GET là placeholder minh họa, không được dùng nguyên xi. Lấy `entity_anchor`, `section`, `topic`, `title` và `code` thực tế từ index và release tương ứng. Chỉ coordinator đáng tin cậy mới được gọi `pin_intent`; tuyệt đối không trao quyền pin intent cho mô hình ngôn ngữ. GET chỉ trả nội dung khi mọi kiểm tra đều pass. FTS5 chỉ tìm ứng viên, không cấp quyền truy xuất.

## 5. Kiểm tra và đo dung lượng

```sh
python -W error::ResourceWarning -m unittest discover -s crs/tests -v
python -m crs.benchmarks.benchmark_large_codex --words 100000
```

Xem `crs/README.md` để hiểu các chỉ tiêu physical bytes: `page_count * page_size` cho database, phân bổ `dbstat` cho FTS/index/manifest, cùng phân loại hot/archive/staging. Các thành phần con trong registry **không được cộng hai lần** vào tổng.

## 6. Checklist bắt buộc trước khi bàn giao

- [ ] Nguồn Markdown UTF-8 có heading và section rõ ràng; RULE đã được kiểm tra điều kiện và ngoại lệ.
- [ ] Mã CDX/release hợp lệ, không trùng hoặc tái sử dụng.
- [ ] Ingest report không có orphan title, dangling anchor hay lỗi chunk.
- [ ] VERIFY pass; shard mới không vượt giới hạn 2 GB.
- [ ] `CURRENT` chỉ đổi sau COMMIT thành công; release trước vẫn đọc được.
- [ ] Chạy toàn bộ test suite và lưu output khi thay đổi code.
- [ ] Thử GET thật với session và intent đã pin, kiểm tra `status == "OK"`.
- [ ] Lưu lại release ID, manifest, report dung lượng và nguồn codex tương ứng.

**Giới hạn hiện tại:** đây là foundation offline single-writer, chưa phải chứng nhận production. Việc phân loại không gắn nhãn vẫn mang tính heuristic; archive tier chưa tự động di chuyển; benchmark tổng hợp không đại diện cho mọi codex thực tế.
