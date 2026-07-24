# E-commerce Lakehouse Pipeline — Batch Local

Repo này là phiên bản hoàn chỉnh của **giai đoạn batch chạy ở local** trong lộ trình xây dựng Azure Lakehouse:

```text
PostgreSQL OLTP
      │ transactional triggers + JDBC event cursor from Delta commit metadata
      ▼
Bronze Delta (append-only change events)
      │ Delta Change Data Feed by table version
      ▼
Silver Delta (one current row per source PK)
      │ dimensional transformations
      ▼
Gold Delta (star schema + fact_sales)
      │ quality checks + financial reconciliation
      ▼
Local analytics-ready Lakehouse
```

Spark/JDBC compute chạy local; Lakehouse có thể đặt trên local filesystem hoặc ADLS Gen2. Phạm vi hiện tại chưa gồm
ADF, Event Hubs, Debezium/Kafka, Unity Catalog hay Power BI. Business rules được tách khỏi infrastructure để có thể
tái sử dụng cho luồng CDC ở phase tiếp theo.

## 1. Những tính chất pipeline bảo đảm

- Cấu hình `base + environment overlay + .env`, validate fail-fast bằng Pydantic.
- Password không nằm trong YAML và bị che khi in object cấu hình.
- PostgreSQL trigger ghi `INSERT`, `UPDATE`, `DELETE` vào `change_events` trong cùng transaction business.
- Bronze đọc JDBC incremental theo khoảng `event_id`; cursor được ghi atomically trong `userMetadata` của chính
  Delta append commit, không có checkpoint file riêng.
- Bronze append-only giữ đầy đủ source payload, trừ cột nhạy cảm khai báo rõ ràng như `password_hash`.
- Column filtering cho analytics chỉ diễn ra ở Silver; Bronze bổ sung `_record_hash`, `_batch_id`, `_ingested_at`
  và source metadata.
- Retry an toàn: dữ liệu và Bronze cursor cùng thuộc một Delta transaction.
- Silver đọc Change Data Feed theo các Bronze Delta version chưa xử lý, chọn event mới nhất theo source PK và
  merge SCD1 vào Delta.
- Silver lưu `_bronze_event_id`; Bronze version đã xử lý nằm trong commit metadata của Silver `_delta_log`.
- Gold đọc Silver CDF, lan truyền affected IDs qua các dependency và chỉ dựng lại dimension members/order facts
  bị ảnh hưởng.
- Gold có 10 dimensions và `fact_sales` ở grain một dòng cho mỗi `order_item`.
- `dim_customer` dùng SCD Type 2; fact lookup customer key theo thời điểm đơn hàng.
- Discount cấp dòng và cấp order được tách đúng, allocation có xử lý rounding residual.
- Gold full build kiểm tra toàn bộ; incremental build chỉ quality-check affected fact rows. `make validate` luôn
  kiểm tra đầy đủ PK, references, SCD2 và đối soát tiền.
- Local pipeline dùng lock để ngăn hai writer chạy đồng thời.

## 2. Cấu trúc repo

```text
azure-lakehouse-pipeline/
├── configs/
│   ├── base.yaml                 # cấu hình chung, dùng env placeholders
│   └── local.yaml                # Spark local + filesystem lakehouse
├── schema/
│   ├── oltpSchema.sql            # PostgreSQL source schema
│   └── dwhSchema.sql             # physical reference cho Gold star schema
├── src/ecommerce_pipeline/
│   ├── ingestion/batch/          # PostgreSQL change events → full-fidelity Bronze
│   ├── pipelines/                # Silver/Gold orchestration and quality gates
│   ├── transformations/          # pure Silver/Gold DataFrame transformations
│   ├── contracts/                # layer-specific Bronze/Silver contracts
│   ├── adapters/                 # PostgreSQL and Delta Lake I/O
│   ├── control/                  # run status and local lock
│   ├── validation/               # cross-layer validation
│   ├── jobs/                     # CLI entry points
│   ├── config/                   # validated configuration
│   ├── runtime/                  # Spark session builder
│   └── generator/                # deterministic source-data generator
├── tests/
│   ├── unit/
│   ├── integration/
│   └── e2e/
├── data/lakehouse/               # generated Delta tables, không commit Git
├── docker-compose.yaml
├── pyproject.toml
├── Makefile
├── .env.example
└── README.md
```

## 3. Yêu cầu máy local

- Python 3.11 hoặc 3.12.
- Java 17.
- Docker Engine/Desktop có Docker Compose v2.
- Tối thiểu khoảng 4 GB RAM trống cho PostgreSQL + Spark local.

Kiểm tra:

```bash
python3 --version
java -version
docker --version
docker compose version
```

## 4. Chạy từ đầu đến cuối

### Bước 1 — tạo environment file

```bash
cp .env.example .env
```

Nội dung mặc định cho local:

```dotenv
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=ecommerce
POSTGRES_USER=admin
POSTGRES_PASSWORD=admin
```

Không commit `.env`. Trên Azure, các giá trị này sẽ được inject từ Key Vault/secret scope thay vì sửa code.

Nếu muốn ghi lakehouse lên Azure Data Lake Storage Gen2 bằng account key, thêm các biến:

```dotenv
AZURE_STORAGE_ACCOUNT=yourstorageaccount
AZURE_CONTAINER=lakehouse
AZURE_STORAGE_AUTH_TYPE=account_key
AZURE_STORAGE_ACCOUNT_KEY=<from-key-vault-or-set-here>
```

Chạy với Azure overlay:

```bash
CONFIG=configs/azure.yaml make run-batch-local
```

`configs/azure.yaml` sẽ dựng path dạng `abfss://<container>@<account>.dfs.core.windows.net/lakehouse` và cấu hình Hadoop ABFS bằng `SharedKey`.

### Bước 2 — tạo virtual environment và cài dependency

```bash
make setup
```

### Bước 3 — khởi tạo PostgreSQL

```bash
make pg-reset
make pg-wait
```

`pg-reset` xóa Docker volume cũ và chạy lại `schema/oltpSchema.sql`. Không dùng lệnh này với database chứa dữ liệu cần giữ.
Sau khi chuyển từ watermark sang change-event, cần reset PostgreSQL và Lakehouse một lần vì contract Bronze đã thay đổi.

### Bước 4 — tạo dữ liệu nguồn có tính lặp lại

```bash
make seed CUSTOMERS=100 ORDERS=500 SEED=42
```

Generator tạo user, address, shop, catalog, voucher, order, order item, payment và shipment. Cùng `SEED` sẽ tạo cùng phân phối dữ liệu.
`seed-stream` tạo insert/update business và xoay voucher marker `CDC_DELETE_*`: marker cũ bị hard delete, marker mới được insert. Nhờ vậy một lần chạy có đủ ba operation để kiểm tra pipeline.

Quy ước tiền:

```text
orders.discount_amount
  = sum(order_items.discount_amount)
  + sum(order_vouchers.discount_amount)

orders.total_amount
  = subtotal + tax + shipping - orders.discount_amount
```

### Bước 5 — chạy toàn bộ batch

```bash
make run-batch-local
```

Job thực hiện tuần tự:

1. Đọc event mới trong `customer_app.change_events` qua JDBC và append vào 12 bảng Bronze.
2. Dựng current-state, normalize và merge vào 12 bảng Silver.
3. Dựng dimensions + `fact_sales`, merge vào Gold.
4. Chạy quality gate và source-to-Gold reconciliation.
5. Ghi run status JSON ở `logs/batch_runs/`, tách khỏi dữ liệu Lakehouse.

Output chính:

```text
data/lakehouse/
├── bronze/batch/<source_table>/
├── silver/<source_table>/
└── gold/
    ├── dim_date/
    ├── dim_time/
    ├── dim_customer/
    ├── dim_location/
    ├── dim_shop/
    ├── dim_category/
    ├── dim_product/
    ├── dim_promotion/
    ├── dim_payment/
    ├── dim_shipping/
    └── fact_sales/
```

Metadata vận hành nằm ngoài Lakehouse:

```text
logs/
├── batch_runs/<batch_id>.json
└── _pipeline.lock               # chỉ tồn tại trong lúc local job đang chạy
```

### Bước 6 — validate kết quả

```bash
make validate
```

Lệnh trả JSON gồm số version Bronze, số current rows Silver, số order/items/fact và tổng gross/discount/tax/shipping/net của Gold. Nếu có sai lệch lớn hơn `0.01`, job fail.

## 5. Chứng minh incremental và idempotency

Chạy lại khi PostgreSQL không thay đổi:

```bash
make run-batch-local
```

Kỳ vọng log của mọi Bronze table có `records=0`; số dòng Silver/Gold không tăng và audit `created_at` của record cũ được giữ nguyên.

Tạo một batch thay đổi gồm order mới và update source hiện có:

```bash
make seed-stream ORDERS_PER_BATCH=2 INTERVAL_SECONDS=0 MAX_BATCHES=1
make run-batch-local
make validate
```

Kỳ vọng chỉ change event mới được append Bronze, gồm cả voucher `DELETE`. Customer đổi thuộc tính tạo thêm một SCD2 version; fact cũ vẫn map customer version đúng theo `order_created_at`.

Có thể chạy toàn bộ demo trên bằng:

```bash
make demo-batch-local
```

## 6. Chạy từng layer khi phát triển

```bash
.venv/bin/python -m ecommerce_pipeline.jobs.run_batch --env local --mode bronze
.venv/bin/python -m ecommerce_pipeline.jobs.run_batch --env local --mode silver
.venv/bin/python -m ecommerce_pipeline.jobs.run_batch --env local --mode gold
```

Chạy một số bảng chỉ hỗ trợ ở mode Bronze/Silver:

```bash
.venv/bin/python -m ecommerce_pipeline.jobs.run_batch \
  --env local --mode bronze --tables orders order_items payments shipments
```

Gold cần đủ source dimensions nên không nhận `--tables`.

Schema Silver chỉ được rebuild khi yêu cầu rõ ràng:

```bash
.venv/bin/python -m ecommerce_pipeline.jobs.run_batch \
  --env local --mode silver --full-rebuild-silver
```

Gold cũng có explicit full rebuild cho schema/business-rule migration:

```bash
.venv/bin/python -m ecommerce_pipeline.jobs.run_batch \
  --env local --mode gold --full-rebuild-gold
```

## 7. Test và quality gates

```bash
make format-check
make lint
make type-check
make test
```

Integration test yêu cầu PostgreSQL đang chạy:

```bash
make pg-up
make pg-wait
make test-integration
```

E2E test reset dữ liệu source trong database local, chạy PostgreSQL → Bronze → Silver → Gold ba lần và kiểm tra incremental/idempotency:

```bash
make test-e2e
```

Lệnh kiểm tra nhanh toàn repo:

```bash
make check
```

## 8. Incremental không dùng checkpoint tự quản

Mỗi bảng có progress độc lập nhưng progress nằm hoàn toàn trong Delta log:

```text
PostgreSQL → Bronze:
  Bronze commitInfo.userMetadata.last_event_id

Bronze → Silver:
  Silver commitInfo.userMetadata.last_processed_bronze_version
  + Bronze Change Data Feed [lastProcessedVersion + 1, latestVersion]

Silver → Gold:
  fact_sales commitInfo.userMetadata.silver_versions
  + Silver Change Data Feed cho từng source table
```

Run bình thường không scan full Bronze Parquet. Silver chỉ đọc các file change của những Delta version mới rồi
`MERGE`. Gold lấy affected IDs từ Silver CDF; payment/shipment/voucher/product changes được lan truyền về đúng
`order_id`, sau đó chỉ các fact rows thuộc order bị ảnh hưởng được dựng lại.

Chỉ lần tạo Silver đầu tiên, lần tự migrate bảng Silver cũ chưa có progress, và `--full-rebuild-silver` đọc full
Bronze snapshot. Bronze cũ được migrate cursor tự động bằng cách đọc `MAX(_event_id)` đúng một lần và ghi marker
vào Delta log. Gold đọc full Silver đúng một lần khi chưa có progress, hoặc khi chạy `--full-rebuild-gold`.

Gold progress được ghi cuối cùng vào commit metadata của `fact_sales`. Nếu job lỗi giữa chừng, progress cũ vẫn
giữ nguyên; retry đọc lại cùng Silver CDF range và các Delta merge/delete theo key vẫn idempotent.

## 9. Mapping sang Azure ở phase tiếp theo

| Local hiện tại | Azure sau này |
|---|---|
| PostgreSQL container | Azure Database for PostgreSQL Flexible Server |
| Spark local | Spark local; chỉ Lakehouse đặt trên ADLS Gen2 |
| `data/lakehouse` | ADLS Gen2 `abfss://...` |
| Delta progress trong `_delta_log`; JSON run status | Delta log trong ADLS; JSON run status nhỏ ngoài Lakehouse |
| `.env` | Key Vault / Databricks secret scope |
| Makefile orchestration | ADF hoặc Databricks Workflows |
| local Delta paths | Unity Catalog external/managed tables |

Máy local vẫn thực hiện JDBC và Spark compute; Azure chỉ chịu chi phí ADLS Gen2. Layer boundaries giữ adapters và
control logic tách khỏi các DataFrame transformations dùng chung cho batch/CDC.

## 10. Giới hạn có chủ đích của batch JDBC

- Trigger/outbox làm tăng write I/O và kích thước PostgreSQL; production chỉ nên xóa event đã qua cursor của mọi consumer.
- `event_id` polling trong project giả định một writer generator. Với OLTP concurrency lớn, dùng PostgreSQL WAL + Debezium thay vì coi sequence ID là commit order tuyệt đối.
- Deploy trigger lên database có sẵn cần backfill snapshot ban đầu trước khi bắt đầu cursor; `pg-reset` local tự giải quyết vì trigger tồn tại trước seed.
- Silver incremental phụ thuộc lịch sử CDF. Cấu hình retention/VACUUM phải giữ các Bronze version đủ lâu cho
  consumer Silver; nếu version cần đọc đã hết retention thì chạy `--full-rebuild-silver`.
- Gold incremental tương tự phụ thuộc Silver CDF retention; nếu version cần đọc đã hết thì chạy
  `--full-rebuild-gold`.
- `schema/dwhSchema.sql` là physical reference; runtime local lưu Gold bằng Delta, không tạo PostgreSQL DWH riêng.

## 11. Troubleshooting

**Thiếu biến môi trường**: loader fail với `Missing required environment variable`. Tạo `.env` từ `.env.example`.

**Port 5432 đang được dùng**: đổi `POSTGRES_PORT` trong `.env`, sau đó chạy lại `make pg-reset`.

**Spark không tải được JAR**: lần đầu cần internet để tải Delta Lake và PostgreSQL JDBC artifacts. Kiểm tra proxy/firewall của Maven Central.

**Muốn chạy lại sạch Lakehouse nhưng giữ PostgreSQL**:

```bash
make lakehouse-reset
make run-batch-local
```

**Pipeline báo đang có writer khác**: đảm bảo không còn job Spark chạy. Nếu job trước bị kill cứng, xóa file generated `logs/_pipeline.lock` rồi chạy lại.
