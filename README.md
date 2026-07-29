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
      │ quality checks → atomic release marker in fact_sales/_delta_log
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
- `dim_customer`, `dim_product`, `dim_shop` và `dim_category` dùng SCD Type 2; fact temporal-join surrogate key
  theo `order_created_at`. Giá và tồn kho sản phẩm được cập nhật Type 1 để không tạo history quá mức.
- Mỗi incremental SCD2 dùng một staged Delta `MERGE`: đóng current version, insert version mới và cập nhật Type 1
  cùng một commit. Job không thể dừng ở trạng thái đã close nhưng chưa insert.
- Discount cấp dòng và cấp order được tách đúng, allocation có xử lý rounding residual.
- Gold full build kiểm tra toàn bộ; incremental build chỉ quality-check affected fact rows. `make validate` luôn
  kiểm tra đầy đủ PK, references, SCD2 và đối soát tiền.
- Gold chỉ publish sau khi quality pass. Một metadata-only commit trên `fact_sales` giữ version chính xác của mọi
  Gold table trong Delta table property `pipeline.goldActiveRelease`;
  consumer đọc bằng snapshot `versionAsOf`, nên không thấy trạng thái nửa batch.
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

JDBC ingestion được giới hạn trong `configs/base.yaml` để không gây burst connection lên PostgreSQL:

```yaml
postgres:
  fetch_size: 10000
  query_timeout_seconds: 300
  connect_timeout_seconds: 15
  socket_timeout_seconds: 300
  max_jdbc_partitions: 4
  target_events_per_partition: 100000
  max_events_per_batch: 500000
  retry_attempts: 3
  retry_initial_backoff_seconds: 2
```

Mỗi source table chụp một upper bound tối đa `max_events_per_batch`, sau đó Spark chia khoảng đó thành số JDBC
partition động và không vượt `max_jdbc_partitions`. Các bảng vẫn chạy tuần tự, vì vậy mặc định PostgreSQL chỉ
phục vụ tối đa bốn connection đọc dữ liệu cùng lúc. Retry dùng exponential backoff và chỉ materialize lại JDBC
DataFrame chưa được commit vào Bronze.

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
User/address được gửi bằng Psycopg pipeline; order được commit theo chunk và in tiến độ để tránh một transaction lớn khi
seed PostgreSQL qua mạng. Có thể điều chỉnh kích thước chunk:

```bash
make seed CUSTOMERS=10000 ORDERS=50000 SEED_BATCH_SIZE=1000
```

`make seed` luôn reset dữ liệu trước khi chạy. Sau baseline, mỗi chunk order được commit độc lập; nếu một chunk lỗi,
chạy lại lệnh sẽ reset và tạo lại dữ liệu deterministic.

Mỗi batch của `seed-stream` chủ động tạo đủ các trường hợp để quan sát pipeline:

- Insert order cùng order items, voucher áp dụng (nếu có), payment và shipment.
- Update customer, product, shop và category để tạo version SCD2 mới ở Gold.
- Update giá/tồn kho product variant theo Type 1.
- Chuyển trạng thái của một order cùng payment và shipment.
- Hard delete một order bootstrap; các bảng con bị xóa cascade để kiểm tra delete ở Silver và xóa fact cũ ở Gold.
- Hard delete voucher marker `CDC_DELETE_*` cũ và insert marker mới để kiểm tra một delete độc lập không có foreign key.

Sau mỗi batch, generator in rõ ID của từng entity đã insert, update và delete. Khi còn order bootstrap, số order nguồn
tăng ròng bằng `ORDERS_PER_BATCH - 1` vì generator vừa thêm order mới vừa xóa một order cũ.

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
3. Dựng candidate dimensions + `fact_sales`, merge vào Gold.
4. Chạy quality gate và source-to-Gold reconciliation.
5. Atomically publish release marker chứa Delta version của toàn bộ Gold tables vào `_delta_log` của `fact_sales`.
6. Ghi run status JSON ở `logs/batch_runs/`, tách khỏi dữ liệu Lakehouse.

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
    └── fact_sales/               # active release nằm trong Delta table property
```

Metadata vận hành nằm ngoài Lakehouse:

```text
logs/
├── batch_runs/<batch_id>.json
└── _pipeline.lock               # chỉ tồn tại trong lúc local job đang chạy
```

Mỗi batch status có `timings_ms.spark_startup`, `bronze`, `silver`, `gold` và `total` để xác định layer chậm mà
không phải suy đoán từ số record. Các timing đã hoàn tất vẫn được giữ nếu batch lỗi.

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

Tạo một batch có đủ insert, update và delete:

```bash
make seed-stream ORDERS_PER_BATCH=2 INTERVAL_SECONDS=0 MAX_BATCHES=1
make run-batch-local
make validate
```

Kỳ vọng chỉ change event mới được append Bronze. Log `seed-stream` cho biết entity nào được thay đổi qua các trường
`inserted_orders`, `scd2_customer`, `scd2_product`, `scd2_shop`, `scd2_category`, `type1_product_variant`,
`advanced_order`, `deleted_order`, `deleted_voucher` và `inserted_voucher`.

Thay đổi customer, product, shop hoặc category tạo SCD2 version mới; fact cũ giữ nguyên dimension version tại
`order_created_at`. Giá/tồn kho variant được cập nhật Type 1. Order bị hard delete biến mất khỏi Silver current-state
và các fact tương ứng cũng bị xóa khỏi Gold.

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

Sau khi nâng schema Gold để thêm SCD2 cho product/shop/category, cần chạy full rebuild Gold đúng một lần.

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
  fact_sales TBLPROPERTIES.pipeline.goldActiveRelease.{silver_versions, gold_versions}
  + Silver Change Data Feed cho từng source table
```

Run bình thường không scan full Bronze Parquet. Silver chỉ đọc các file change của những Delta version mới rồi
`MERGE`. Gold lấy affected IDs từ Silver CDF; payment/shipment/voucher changes được lan truyền về đúng `order_id`,
sau đó chỉ các fact rows thuộc order bị ảnh hưởng được dựng lại. Product/shop/category changes chỉ mở SCD2 version
mới và không rewrite historical facts.

Chỉ lần tạo Silver đầu tiên, lần tự migrate bảng Silver cũ chưa có progress, và `--full-rebuild-silver` đọc full
Bronze snapshot. Bronze cũ được migrate cursor tự động bằng cách đọc `MAX(_event_id)` đúng một lần và ghi marker
vào Delta log. Gold đọc full Silver đúng một lần khi chưa có progress, hoặc khi chạy `--full-rebuild-gold`.

Gold progress và publish state dùng chung một release marker, không có checkpoint file hay Delta table điều phối
riêng. Candidate tables được ghi trước; sau khi quality gate pass, một metadata-only `ALTER TABLE SET TBLPROPERTIES`
commit ghi marker vào `_delta_log` của `fact_sales`. Marker chứa version chính xác của cả 11 Gold tables. Đọc marker
từ current Delta metadata không scan Parquet và cũng không scan toàn bộ history. Consumer chụp marker một lần rồi
đọc mọi table bằng `versionAsOf`: trước commit thấy toàn bộ release cũ, sau commit thấy toàn bộ release mới.

Nếu job lỗi giữa chừng, marker cũ giữ nguyên nên dữ liệu candidate chưa hoàn tất không được publish. Retry đọc
lại cùng Silver CDF range và các Delta merge/delete theo key vẫn idempotent. Trong code Python, entrypoint đọc
analytics-ready Gold là:

```python
snapshot = GoldReleaseStore(spark, config).snapshot()
fact_sales = snapshot.read_table("gold", "fact_sales")
dim_product = snapshot.read_table("gold", "dim_product")
```

Không dùng `spark.read.load(.../gold/...)` trực tiếp cho consumer vì cách đó đọc physical latest version, bao gồm
cả candidate chưa publish. Lakehouse cũ có `_publish_manifest` vẫn được đọc để tương thích; Gold run tiếp theo sẽ
tạo marker native trong `fact_sales/_delta_log`, sau đó thư mục legacy có thể được xóa thủ công.

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
- Gold snapshot phụ thuộc Delta time travel. Không `VACUUM` các version đang được active release marker tham chiếu;
  retention phải dài hơn thời gian tối đa từ lúc candidate bắt đầu đến khi publish/retry hoàn tất.
- Atomic release marker giả định một Gold writer. Local lock đã bảo đảm điều này trên một máy; khi chạy nhiều worker,
  orchestrator phải đặt concurrency bằng 1 hoặc dùng distributed lease.
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
