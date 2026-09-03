# Báo cáo kỹ thuật — Day 28 Track 2

**Học viên:** Nguyễn Chí Hiếu  
**Mã học viên:** 2A202601931  
**Repository:** `Track2-Day28-2A202601931-NguyenChiHieu`

## 1. Phạm vi thực hiện

Repository ban đầu đã cung cấp kiến trúc nền, dịch vụ Docker Compose, pipeline
Airflow/Spark, API, telemetry, manifest Kubernetes/GitOps và bộ kiểm thử. Phần
triển khai trực tiếp của học viên tập trung vào bốn boundary trong
`src/lab28_platform/integration_tasks.py`:

1. Tạo Kafka headers cho `traceparent` và `idempotency-key`.
2. Khử trùng lặp sự kiện theo `idempotency_key`, chọn bản mới nhất bằng
   `(occurred_at, event_id)` và trả kết quả theo thứ tự xác định.
3. Tạo request đúng contract của Feast feature service.
4. Tổng hợp probe thành `ready`, `degraded` hoặc `not_ready` theo mức độ bắt buộc.

Ngoài phần code trên, học viên vận hành stack, chạy các cổng kiểm tra và thu thập
evidence. Báo cáo phân biệt rõ phần được hiện thực hóa với phần đã có trong
scaffold; việc khởi chạy một service không được xem là đã tự triển khai service đó.

## 2. Kiến trúc và quyền sở hữu

```text
Client
  │
  ▼
Envoy Gateway ──► FastAPI
                    ├─ ingest ─► Kafka ─► Airflow ─► Spark/Delta
                    │                                  ├─► Feast online store
                    │                                  └─► Qdrant index
                    └─ ask ───► MLflow champion
                               + Feast features
                               + Qdrant retrieval
                                      │
                                      ▼
                               grounded prompt ─► vLLM

Metrics ─► Prometheus ─► Grafana
Spans   ─► OTel Collector ─► Jaeger; LangSmith là gate có credential
```

| Nhóm trách nhiệm | Boundary | Phần đã thực hiện/kiểm chứng |
|---|---|---|
| Ingestion & Orchestration | IP01–IP02 | Kafka headers, key contract, DAG run và asset event |
| Data & ML | IP03–IP04–IP06 | Dedupe, Delta version, Feast request và MLflow alias |
| Serving & Retrieval | IP05–IP07 | Qdrant retrieval và gate danh tính vLLM |
| Platform & Observability | IP08–IP10 | Readiness, gateway, metrics/dashboard và trace coverage |

## 3. Trạng thái 10 điểm tích hợp

Trạng thái dưới đây dựa trên evidence đã commit, không suy diễn từ việc unit test
đạt. `PARTIAL` nghĩa là đã có tín hiệu hoạt động nhưng artifact chưa chứng minh đủ
Definition of Done.

| IP | Trạng thái | Nhận xét |
|---|---|---|
| IP01 | PARTIAL | Message và trace context đã có; record/header key trong evidence chưa trùng `value.idempotency_key`. |
| IP02 | PASS | Có DAG run thành công, task states và asset events. |
| IP03 | PARTIAL | Có Delta version/time travel; cần MERGE history và replay row-count proof. |
| IP04 | PASS | Có entity, feature values, Delta version và freshness. |
| IP05 | PASS | Có collection, model revision, hybrid scores và deterministic document IDs. |
| IP06 | PARTIAL | Champion resolve được; evidence cần kèm signature, provenance tags và Delta version hợp lệ. |
| IP07 | PASS | Đã triển khai extension trên Kaggle NVIDIA Tesla T4x2 (vLLM 0.28.0, Qwen/Qwen2.5-0.5B-Instruct), thu thập thành công `/version`, `/v1/models`, 66 `vllm:` metrics và chat completion thật (`ip07-chat-completion.json`). |
| IP08 | PASS | Có route 200, rate-limit 429 và `x-request-id`. |
| IP09 | PARTIAL | Prometheus/Grafana/alerts hoạt động; target vLLM phải `up` khi chạy GPU gate. |
| IP10 | PARTIAL | Trace serving hiện có; cần một trace nối cả ingest, Kafka, Airflow, Spark và serving. |

IP07 đã được hoàn thành thông qua Kaggle GPU Extension (`hiengchi/lab28-vllm-serving` trên Tesla T4), giải quyết trọn vẹn yêu cầu môi trường inference thật cho vLLM. `integration-report.json` và bộ evidence phản ánh trung thực trạng thái kiểm thử thực tế.

## 4. Các đánh đổi kỹ thuật

### Kafka thay cho ghi trực tiếp

Kafka thêm chi phí vận hành và một bước bất đồng bộ, đổi lại tạo buffer khi tải
tăng và cho phép replay. Scaffold dùng record key để giữ thứ tự theo thực thể và
`idempotency_key` trong payload làm logical identity cho Delta. Evidence hiện còn
bất nhất giữa record key, header và payload nên chưa chứng minh trọn vẹn IP01.

### Delta MERGE và versioning

MERGE theo `idempotency_key` làm cho at-least-once delivery an toàn hơn và version
cho phép truy lại dữ liệu của một release. Chi phí là transaction log và các file
cũ phải được quản lý bằng retention/VACUUM phù hợp.

### Feast và Qdrant là hai nhánh khác nhau

Feast phục vụ đặc trưng theo entity; Qdrant phục vụ tài liệu nền cho grounding.
Hai hệ thống không phụ thuộc tuần tự vào nhau. Online materialization giảm độ trễ
đọc nhưng tạo yêu cầu giám sát freshness; hybrid retrieval tăng chi phí index để
cải thiện truy vấn có từ khóa/tên riêng. Chưa có benchmark chất lượng nên không
khẳng định mức cải thiện retrieval.

### MLflow alias cho promotion/rollback

Alias `champion` cho phép đổi release mà không rebuild image. Để rollback có thể
tái lập, mỗi version cần lưu prompt version, model IDs, retrieval config, Delta
version, Git SHA, signature và evaluation metrics.

## 5. Sự cố và khôi phục

Thiết kế có DLQ cho payload không hợp lệ, commit Kafka offset sau durable write và
MERGE idempotent để replay không nhân bản dữ liệu. Feast là dependency có thể suy
giảm; Qdrant và vLLM là bắt buộc trong profile production-ready.

Mô tả cơ chế không thay thế bằng chứng. Hồ sơ hoàn chỉnh cần ghi lại ít nhất một
failure injection với trạng thái trước/sau, DLQ/replay count, DAG run ID và số dòng
Delta chứng minh không mất hoặc nhân bản dữ liệu.

## 6. Kết quả kiểm thử tải

Load probe gửi **200 yêu cầu với 8 worker** đến `/ready`; đây không phải 200 yêu
cầu đồng thời và cũng chưa đại diện cho latency của `/api/v1/ask`:

- P50: 798,05 ms
- P95: 1.256,76 ms
- P99: 3.042,23 ms
- 200/200 phản hồi HTTP 200; body ở trạng thái `degraded` khi vLLM chưa sẵn sàng

Probe đi qua Envoy và thực hiện tuần tự năm dependency checks cho mỗi request, nên
fan-out là điểm nghẽn chính. Có thể cache kết quả probe trong 1–2 giây, nhưng cần
giữ liveness và readiness tách biệt. P95/P99 hiện vượt 1 giây; lần đo tiếp theo
cần bổ sung profile cho `/api/v1/ask`, CPU/RAM, GPU memory, token throughput và
saturation sau khi vLLM sẵn sàng.

## 7. Khoảng cách với production

1. Secret cần chuyển sang Kubernetes Secrets hoặc secret manager và áp dụng cơ
   chế rotation.
2. Kafka, MLflow backend và Feast online store hiện là cấu hình đơn nút; production
   cần HA, backup/restore và kiểm tra phục hồi.
3. vLLM cần capacity planning theo VRAM, context length, concurrency và queue time;
   không mặc định rằng thêm GPU sẽ tự tăng throughput.
4. Kubernetes/GitOps hiện mới được kiểm tra tĩnh; drift detection và rollback cần
   evidence từ một cluster chạy thật.
5. Trace cần nối được cả asynchronous leg và có chính sách sampling/retention phù
   hợp với dữ liệu nhạy cảm.

## 8. Kiểm chứng và đóng góp cá nhân

Các cổng kiểm tra nhanh đã đạt: Ruff, portability, manifest validation, 245 matrix
checks, toàn bộ unit tests và bốn starter tests. Live integration/GPU gates được
báo riêng theo kết quả chạy; test bị deselect hoặc dependency chưa sẵn sàng không
được tính là pass.

**Nguyễn Chí Hiếu (2A202601931):** hoàn thiện bốn integration tasks thuộc phạm vi
sinh viên, vận hành stack, triển khai thành công Kaggle GPU Extension trên cụm Tesla T4x2
(vLLM 0.28.0) cho IP07 và biên soạn evidence theo trạng thái quan sát được.
