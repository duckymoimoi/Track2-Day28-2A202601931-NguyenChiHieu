# Day 28 Track 2 — Báo cáo kỹ thuật

**Học viên:** Nguyễn Chí Hiếu  
**Mã học viên:** 2A202601931  
**Repository:** `Track2-Day28-2A202601931-NguyenChiHieu`

## 1. Phần thực hiện

- `event_headers`: tạo Kafka headers cho `traceparent` và `idempotency-key`.
- `dedupe_latest`: giữ sự kiện mới nhất theo `idempotency_key`, so sánh bằng
  `(occurred_at, event_id)` và trả kết quả theo thứ tự key xác định.
- `feast_online_request`: tạo request đúng feature references của Feast.
- `readiness_status`: trả `ready`, `degraded` hoặc `not_ready` theo trạng thái và
  mức độ bắt buộc của dependency.
- Vận hành stack, chạy validation và thu thập evidence cho các integration point.
- Chạy Kaggle kernel version 10 để kiểm chứng IP07 bằng vLLM và GPU thật.

## 2. Kiến trúc và ownership

```text
Client
  │
  ▼
Envoy ─► FastAPI
           ├─ ingest ─► Kafka ─► Airflow ─► Spark/Delta
           │                                ├─► Feast
           │                                └─► Qdrant
           └─ ask ─► MLflow champion + Feast + Qdrant ─► prompt ─► vLLM

Metrics ─► Prometheus ─► Grafana
Spans   ─► OTel Collector ─► Jaeger / LangSmith
```

| Ownership | Boundary | Evidence chính |
|---|---|---|
| Ingestion & Orchestration | IP01–IP02 | Kafka message, trace header, DAG run, asset event |
| Data & ML | IP03–IP04–IP06 | Delta version, Feast feature row, MLflow champion |
| Serving & Retrieval | IP05–IP07 | Qdrant result, vLLM identity và completion |
| Platform & Observability | IP08–IP10 | Gateway, metrics/dashboard, distributed trace |

## 3. Trạng thái integration point

| IP | Trạng thái | Kết quả |
|---|---|---|
| IP01 | PARTIAL | Có Kafka message và trace context; record/header key chưa trùng `value.idempotency_key`. |
| IP02 | PASS | DAG run thành công, các task thành công và có asset events. |
| IP03 | PARTIAL | Có Delta version và time travel; chưa có MERGE/replay row-count proof. |
| IP04 | PASS | Có entity, feature values, Delta version và freshness. |
| IP05 | PASS | Có hybrid result, score, document ID và embedding model revision. |
| IP06 | PARTIAL | Champion resolve được và có signature/tags; `git_sha` trống và `delta_version` chưa hợp lệ. |
| IP07 | PASS | Kaggle version 10 chạy trên hai Tesla T4; vLLM 0.28.0 phục vụ đúng `Qwen/Qwen3-1.7B`, có 66 `vllm:` metrics và completion trả `vLLM` với `finish_reason=stop`. |
| IP08 | PASS | Gateway trả 200, rate limit trả 429 và cả hai có `x-request-id`. |
| IP09 | PARTIAL | Prometheus, Grafana và alert rules hoạt động; target vLLM của stack local chưa `up`. |
| IP10 | PARTIAL | Trace serving có đủ synchronous spans; còn thiếu ingest, Kafka, Airflow và Spark spans trên cùng trace. |

## 4. Trade-offs

| Quyết định | Lợi ích | Chi phí/giới hạn |
|---|---|---|
| Kafka thay cho ghi trực tiếp | Buffer tải, replay và tách ingestion khỏi xử lý dữ liệu | Tăng độ trễ và chi phí vận hành broker/consumer |
| Delta MERGE theo `idempotency_key` | Replay không tạo bản ghi trùng; hỗ trợ version/time travel | Transaction log và file cũ cần retention/VACUUM |
| Feast online store | Tra cứu feature theo entity với độ trễ ổn định | Phải materialize và giám sát freshness |
| Qdrant hybrid retrieval | Kết hợp semantic và lexical retrieval | Tăng chi phí embedding, index và truy vấn |
| MLflow alias `champion` | Promotion/rollback không cần rebuild image | Release phải lưu đủ model, prompt, retrieval và data provenance |
| vLLM trên Kaggle | Kiểm chứng GPU/vLLM thật khi local không tương thích | Session có thời hạn; không phù hợp làm endpoint production |

## 5. Failure và recovery

- Payload không hợp lệ được chuyển vào `data.raw.dlq` cùng thông tin lỗi.
- Kafka offset chỉ commit sau durable processing.
- Delta MERGE theo `idempotency_key` hỗ trợ replay không nhân bản dữ liệu.
- Feast failure cho phép phản hồi `degraded`.
- Qdrant hoặc vLLM failure làm dependency bắt buộc không sẵn sàng.
- Evidence còn thiếu failure-injection record gồm before/after state, DLQ/replay
  count, DAG run ID và Delta row count.

## 6. Load profile

| Thuộc tính | Kết quả |
|---|---:|
| Target | `http://localhost:8080/ready` |
| Requests | 200 |
| Workers | 8 |
| HTTP 200 | 200/200 |
| P50 | 798,05 ms |
| P95 | 1.256,76 ms |
| P99 | 3.042,23 ms |

- Response body ở trạng thái `degraded` do vLLM local không sẵn sàng.
- `/ready` kiểm tra tuần tự năm dependency; fan-out là bottleneck chính.
- Kết quả này không đại diện cho latency của `/api/v1/ask`.
- Cải tiến đề xuất: cache probe 1–2 giây và profile thêm request inference thật.

## 7. Production gaps

| Hạng mục | Khoảng cách cần xử lý |
|---|---|
| Secrets | Dùng Kubernetes Secrets/secret manager và rotation |
| Kafka/MLflow/Feast | Bổ sung HA, backup và restore testing |
| vLLM | Capacity planning theo VRAM, context length, concurrency và queue time |
| Kubernetes/GitOps | Cần live drift detection và rollback evidence |
| Tracing | Nối asynchronous spans; cấu hình sampling, retention và bảo vệ dữ liệu nhạy cảm |
| Performance | Đo `/api/v1/ask`, CPU/RAM, GPU memory, token throughput và saturation |

## 8. Contribution

**Nguyễn Chí Hiếu (2A202601931):** hoàn thiện bốn integration tasks thuộc phạm vi
sinh viên; vận hành stack; thu thập evidence; kiểm chứng IP07 bằng Kaggle kernel
version 10 trên hai Tesla T4 với vLLM 0.28.0 và `Qwen/Qwen3-1.7B`.
