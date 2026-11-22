# Backrooms
Backrooms

## Codex Retrieval System (CRS v3.0)

Hệ thống nạp và truy xuất Codex offline nằm trong [`crs/`](crs/). Người nhập Codex mới **đọc [hướng dẫn nhập Codex bằng tiếng Việt](crs/CODEX_INGEST_GUIDE.vi.md)** trước khi chuẩn bị Markdown, cấp mã, BUILD, VERIFY hoặc publish release. Tài liệu kỹ thuật và API: [`crs/README.md`](crs/README.md).

Kiểm thử từ thư mục gốc repository:

```sh
python -W error::ResourceWarning -m unittest discover -s crs/tests -v
```
