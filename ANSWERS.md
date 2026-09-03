# Báo cáo Trả lời & Thiết kế Kiến trúc nền tảng AI — Day 28 Track 2

**Học viên:** Nguyễn Chí Hiếu  
**Mã học viên:** 2A202601931  
**Repository:** `Track2-Day28-2A202601931-NguyenChiHieu`  
**Nhóm vai trò:** Ingestion & Orchestration, Data & ML, Serving & Retrieval, Platform & Observability  

---

## 1. Kiến trúc Tổng thể & 10 Điểm Tích hợp (Integration Points)

Hệ thống được thiết kế theo mô hình 5 tầng (5-Layer Modern AI Platform) chuẩn công nghiệp:

```
[L1 Compute / Ingress]
       │
  Envoy Gateway (:8080) ───(Rate Limit / Trace Header)───► FastAPI Ingestion / Serving (:8000)
       │                                                         │
[L2 Data Pipeline]                                               │
       ▼                                                         ▼
Kafka (:9092) ───► Airflow 3 (:8082) ───► Apache Spark ───► Delta Lake (.lab28/delta)
                                                                 │
[L3 ML & Feature Store]                                         │
       ┌─────────────────────────────────────────────────────────┘
       ▼
Feast Online Store (:6566) ───► Qdrant Hybrid Vector DB (:6333) ───► MLflow Registry (:5000)
       │                                                                  │
[L4 Serving & LLM]                                                        │
       └─────────────────────────► Grounded Prompt ◄──────────────────────┘
                                          │
                                          ▼
                                   vLLM Engine (:8001)
                                          │
[L5 Observability & Operations]           ▼
OpenTelemetry Collector (:4317) ──► Jaeger (:16686) / Prometheus (:9090) ──► Grafana (:3000)
```

### Bảng tóm tắt 10 Điểm tích hợp (IP01 - IP10):
1. **IP01 (API → Kafka):** API ingest feedback/documents, gắn `traceparent` và `idempotency-key` vào Kafka headers, đẩy vào topic `data.raw`.
2. **IP02 (Kafka → Airflow 3):** Airflow DAG `lab28_ingestion_pipeline` tiêu thụ theo lô từ `data.raw`, phát sinh asset event `lab28://delta/feedback`.
3. **IP03 (Pipeline → Delta Lake):** Spark thực hiện MERGE vào Delta Lake (`feedback` và `documents`), hỗ trợ ACID transactions và time travel.
4. **IP04 (Delta → Feast Feature Store):** Trích xuất snapshot offline tại `delta_root/exports/asker_activity`, Feast materialize vào SQLite online store cho entity `asker_id`.
5. **IP05 (Data → Qdrant Vector Store):** Sinh embeddings từ mô hình đa ngữ `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2` và BM25 sparse vectors, upsert với UUID xác định theo `doc_id`.
6. **IP06 (MLflow → Model Registry):** Quản lý metadata, artifact, prompt template v1, đăng ký mô hình `lab28-rag-release` và gán alias `champion`.
7. **IP07 (Serving → vLLM):** Điểm cuối suy luận LLM xác thực qua endpoint `/version` và họ metrics `vllm:`.
8. **IP08 (Serving → Envoy Gateway):** Cổng vào thực thi giới hạn tốc độ (token bucket 10 req/s), đính kèm `x-request-id`, loại trừ pod `not_ready` qua probe `/ready`.
9. **IP09 (All → Prometheus & Grafana):** Thu thập metrics toàn cụm (`lab28_request_seconds`, `lab28_feature_lookup_seconds`, `envoy_http_downstream_rq_total`), cấu hình SLO alerts.
10. **IP10 (All → OpenTelemetry & Jaeger Tracing):** Truy vết phân tán liên tục xuyên suốt từ Gateway, Ingestion, Kafka, Delta, Feast, Qdrant đến LLM với cùng 1 `trace_id`.

---

## 2. Phân tích Đánh đổi Kỹ thuật (Trade-offs)

### 2.1. Kafka Ingestion vs Direct Database Writes
- **Đánh đổi:** Thay vì ghi trực tiếp vào cơ sở dữ liệu, việc đưa qua Kafka làm tăng độ trễ ghi tức thời (vài ms) và đòi hỏi quản lý cluster Kafka (Zookeeper/KRaft, retention, consumer lag).
- **Lợi ích:** Tạo lớp đệm chịu tải (buffering), giải phóng áp lực cho hạ tầng dữ liệu phía sau khi có burst traffic. Khả năng phát lại (replayability) giúp khắc phục sự cố mà không làm mất dữ liệu người dùng (`no-data-loss`).

### 2.2. Delta Lake ACID & Versioning vs Chi phí Lưu trữ
- **Đánh đổi:** Mỗi commit MERGE tạo ra file Parquet mới và file log JSON trong `_delta_log/`, tốn dung lượng ổ đĩa nếu không chạy `VACUUM` thường xuyên.
- **Lợi ích:** Đảm bảo tính toàn vẹn dữ liệu (ACID MERGE), tránh trùng lặp bản ghi khi Kafka replay nhờ `idempotency_key`. Tính năng Time Travel cho phép tái hiện chính xác tập dữ liệu huấn luyện tương ứng với model release cụ thể.

### 2.3. Feast Online Store vs Query On-demand
- **Đánh đổi:** Cần định kỳ chạy `materialize_incremental` để đẩy đặc trưng từ Lakehouse sang Online Store (SQLite/Redis), dẫn đến độ trễ dữ liệu (freshness delay).
- **Lợi ích:** Độ trễ đọc đặc trưng phục vụ suy luận cực thấp (< 5ms), đáp ứng ngân sách độ trễ (latency budget) khắt khe của hệ thống phục vụ người dùng thời gian thực.

### 2.4. Qdrant Hybrid Search (Dense + Sparse BM25) vs Dense-only Search
- **Đánh đổi:** Tốn thêm tài nguyên tính toán để sinh cả vector ngữ nghĩa dày (dense) và vector từ khóa thưa (sparse BM25), tăng kích thước chỉ mục.
- **Lợi ích:** Khắc phục nhược điểm của dense embedding đối với các mã tra cứu cụ thể, tên riêng, thuật ngữ kỹ thuật, nâng cao đáng kể độ chính xác RAG.

---

## 3. Kịch bản Sự cố & Khôi phục (Failure & Recovery)

### 3.1. Thông điệp độc hại (Poison Message Handling)
- **Cơ chế:** Khi Kafka consumer gặp bản ghi hỏng (sai schema hoặc payload lỗi không parse được), hệ thống không dừng cả pipeline mà chuyển hướng bản ghi vào `data.raw.dlq` (Dead Letter Queue), kèm lý do lỗi trong header. Consumer tiếp tục xử lý các bản ghi hợp lệ kế tiếp mà không bị nghẽn (no poison pill blockage).

### 3.2. Suy thoái có kiểm soát (Graceful Degradation)
- **Cơ chế:** Khi Feature Store (Feast) chưa kịp khởi động hoặc entity mới chưa có lịch sử, API không trả về lỗi 500 mà trả về trạng thái `degraded` với các giá trị mặc định an toàn (`avg_rating=0.0`, `feedback_count=0`). Khi vLLM không khả dụng trên môi trường CPU/standard, endpoint `/ready` trả về `degraded` (HTTP 200) thay vì loại bỏ pod hoàn toàn, đảm bảo khả năng quan sát và thử nghiệm.

---

## 4. Kết quả Kiểm thử Tải (Load Profile & Bottlenecks)

Kiểm thử tải với 200 yêu cầu đồng thời (8 workers) vào endpoint `/ready`:
- **P50 Latency:** 618.45 ms
- **P95 Latency:** 1020.24 ms
- **P99 Latency:** 1304.42 ms
- **Success Rate:** 100% (200/200 requests thành công với mã HTTP 200)

**Phân tích điểm nghẽn (Bottleneck Analysis):**
1. **Probe Fan-out:** Endpoint `/ready` kiểm tra đồng thời 5 thành phần (Kafka, MLflow, Feast, Qdrant, vLLM). Khi tải cao, các kết nối TCP/HTTP nối tiếp tạo ra độ trễ cộng dồn.
2. **Khuyến nghị Production:** Bổ sung cơ chế in-memory caching với TTL ngắn (1 - 2 giây) cho kết quả probe readiness để giảm tải truy vấn cho các downstream services.

---

## 5. Khoảng cách với Môi trường Production (Production Gaps)

1. **Bảo mật & Quản lý Secret:** Hiện tại mật khẩu Airflow, Grafana được lưu trữ qua environment variables và volume mount file cục bộ. Cần chuyển sang HashiCorp Vault hoặc Kubernetes Secrets / AWS Secrets Manager.
2. **Scale & HA cho Storage:** Feast đang dùng SQLite cho online store; môi trường production cần chuyển sang cụm Redis Cluster phân tán.
3. **Kafka & Spark Cluster:** Triển khai Kafka đa broker với replication factor >= 3 trên Kubernetes sử dụng Strimzi Operator. Spark chạy trên chế độ Kubernetes Native Mode hoặc EMR/Databricks.
4. **Hạ tầng Tăng tốc GPU:** Triển khai vLLM với cấu hình Multi-GPU Tensor Parallelism (NVIDIA H100/A100) và áp dụng Dynamic LoRA serving cho các tác vụ chuyên biệt.

---

## 6. Đóng góp & Trách nhiệm Cá nhân (Contribution)

**Nguyễn Chí Hiếu (2A202601931):**
- Đảm nhận toàn bộ chu trình kỹ thuật:
  - Hiện thực hóa 4 hàm biên tích hợp cốt lõi trong `src/lab28_platform/integration_tasks.py` (`event_headers`, `dedupe_latest`, `feast_online_request`, `readiness_status`).
  - Triển khai và vận hành Docker Compose stack (12 container cốt lõi: Kafka, Qdrant, MLflow, Feast, API, Envoy Gateway, Prometheus, Grafana, Jaeger, OTel Collector, PushGateway, Kafka Exporter).
  - Thu thập đầy đủ 10 tệp bằng chứng kỹ thuật (`evidence/ip01` đến `evidence/ip10`) đáp ứng 100% tiêu chí Milestone 3.
  - Vượt qua toàn bộ 83 test trong bộ kiểm thử `tests`, 4 test trong `starter-tests`, và 245 kiểm tra hợp chuẩn trong `verify_matrix.py`.
