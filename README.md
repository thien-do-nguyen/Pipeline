# E-commerce Lakehouse Pipeline — Local và Azure Databricks

Repo này chạy batch pipeline trên Spark local/Azure Databricks và PostgreSQL CDC streaming ở local:

```text
                         PostgreSQL OLTP
                         /             \
       JDBC trigger/outbox CDC        WAL / Debezium / Kafka
                   │                           │
                   ▼                           ▼
       Bronze Batch Raw              Bronze Raw CDC
       bronze/batch/<table>           bronze/cdc_events
                   \                         /
                    \       12 Typed Bronze + quarantine
                     \      bronze/streaming/<table>
                      \                  /
                       ▼                ▼
                  Unified Silver (một row hiện tại/PK + tombstone)
                                  │
                                  ▼
                  Unified Gold (star schema + fact_sales)
                                  │
                                  ▼
                       Analytics-ready Lakehouse
```

Nhánh batch không quét full 12 bảng OLTP: trigger ghi outbox `change_events`, Spark/JDBC chỉ đọc khoảng
`event_id` sau cursor của từng bảng. Full snapshot chỉ là thao tác bootstrap/backfill có chủ đích, không phải
chế độ batch thường xuyên.

Spark/JDBC compute có thể chạy local hoặc trên Databricks. Local dùng filesystem; cloud dùng external Delta tables
trên StorageV2/ADLS Gen2 do project sở hữu. Unity Catalog quản lý metadata, permissions, lineage và governance.
Power BI có thể đọc Gold qua Databricks SQL. Local CDC sử dụng Kafka KRaft và Debezium Connect, trong khi Cloud CDC sử dụng Azure Event Hubs (Kafka API) kết hợp ACI Debezium và Databricks Structured Streaming được quản lý tự động qua Terraform trong `infra/cloud/cdc`.

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
- Silver là điểm hội tụ duy nhất của Batch Bronze và Streaming CDC Bronze. Cả hai dùng chung transformation và
  cùng ghi một Delta table cho mỗi entity.
- Silver chọn event theo `event_occurred_at`, ưu tiên CDC khi hòa, rồi sequence của nguồn; tombstone được giữ vật lý
  để batch cũ không thể làm sống lại record đã bị CDC xóa.
- Silver chỉ materialize append-only change-history cho 5 nguồn SCD2 (`app_users`, `shops`, `categories`, `products`,
  `product_variants`). Bảy entity giao dịch còn lại dùng append-only Bronze làm audit trail và Silver CDF làm nguồn
  incremental, tránh 7 Delta commits không có downstream consumer trong mỗi batch.
- Reader nghiệp vụ loại `_is_deleted=true`; Gold chỉ đọc Unified Silver và không biết dữ liệu đến từ batch hay stream.
- Một batch truyền Bronze Delta versions trực tiếp sang Silver và Silver versions trực tiếp sang Gold, tránh đọc
  lại history của 12 bảng ở layer kế tiếp.
- Nếu toàn bộ Bronze tables không có event mới, `--mode all` kết thúc ngay sau Bronze và không scan metadata
  Silver/Gold.
- Gold đọc Silver CDF, lan truyền affected IDs qua các dependency và chỉ dựng lại dimension members/order facts
  bị ảnh hưởng.
- Incremental Gold chỉ append member mới cho các dimension bất biến `dim_date`, `dim_time`, `dim_location`,
  `dim_payment` và `dim_shipping`; unknown member chỉ được tạo ở full build. Cách này tránh Delta `MERGE` rewrite
  file khi micro-batch chỉ tham chiếu member đã tồn tại.
- Gold có 10 dimensions và `fact_sales` ở grain một dòng cho mỗi `order_item`.
- `dim_customer`, `dim_product`, `dim_shop` và `dim_category` dùng event-level SCD Type 2 từ immutable Silver
  change history; mọi intermediate business version giữa hai Gold runs được replay theo source order. Fact
  temporal-join surrogate key theo `order_created_at`. Giá và tồn kho sản phẩm là Type 1.
- Incremental SCD2 replay toàn bộ history của entity bị ảnh hưởng rồi idempotent upsert theo deterministic surrogate
  key. Parent category rename tạo version mới cho child; product dựng temporal join giữa product và variant history.
- Discount cấp dòng và cấp order được tách đúng, allocation có xử lý rounding residual.
- Gold full build kiểm tra toàn bộ; incremental build chỉ quality-check affected fact rows. `make validate` luôn
  kiểm tra đầy đủ PK, references, SCD2 và đối soát tiền.
- Gold chỉ publish sau khi quality pass. Một metadata-only commit trên `fact_sales` giữ version chính xác của mọi
  Gold table trong Delta table property `pipeline.goldActiveRelease`;
  consumer đọc bằng snapshot `versionAsOf`, nên không thấy trạng thái nửa batch.
- Batch và streaming đều có thể trigger cùng một `GoldBuilder`. Local file lock hoặc Delta-backed cloud lock serialize
  mỗi lần publish; Gold đọc lại Silver versions sau khi lấy lock và tự no-op nếu trigger trước đã xử lý cùng version.
- CDC downstream vật lý hóa 12 typed contracts thành Delta trước khi Silver đọc. Valid records tiếp tục xử lý;
  record lỗi type/schema/key đi vào `bronze/streaming/quarantine` và phát quality alert, không làm chết query.
- Typed Bronze và mỗi Silver target dùng `txnAppId + txnVersion`; sequence guard bảo vệ retry/out-of-order event.

## 2. Cấu trúc repo

```text
azure-lakehouse-pipeline/
├── configs/
│   ├── base.yaml                 # cấu hình chung, dùng env placeholders
│   ├── local.yaml                # Spark local + filesystem lakehouse
│   └── azure.yaml                # Databricks + Unity Catalog
├── schema/
│   ├── oltpSchema.sql            # PostgreSQL source schema
│   └── dwhSchema.sql             # physical reference cho Gold star schema
├── infra/local/
│   ├── airflow/                  # local scheduler + DAG gọi batch entry point hiện có
│   ├── postgres/                 # idempotent CDC role + publication bootstrap
│   ├── kafka/                    # Kafka topics, gồm compacted Connect state topics
│   └── connect/                  # Debezium connector config + registration
├── src/ecommerce_pipeline/
│   ├── ingestion/batch/          # PostgreSQL change events → full-fidelity Bronze
│   ├── ingestion/streaming/      # Kafka source → normalized raw CDC Bronze
│   ├── pipelines/                # Silver/Gold orchestration and quality gates
│   ├── transformations/          # pure Silver/Gold DataFrame transformations
│   ├── contracts/                # layer-specific Bronze/Silver/CDC contracts
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
├── data/lakehouse/               # generated Bronze/Silver/Gold Delta tables, không commit Git
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
- Tối thiểu khoảng 4 GB RAM trống cho batch; nên có 8 GB RAM trống khi chạy thêm Kafka và Kafka Connect.

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

```dotenv
# 1. REQUIRED LOCAL RUNTIME CONFIGURATION
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=ecommerce
POSTGRES_USER=admin
POSTGRES_PASSWORD=admin
CDC_POSTGRES_PASSWORD=cdc-local-only

# 2. OPTIONAL: CUSTOM PORTS & SCHEDULING (Có sẵn default trong docker-compose)
KAFKA_PORT=29092
KAFKA_CONNECT_PORT=8083
AIRFLOW_PORT=8080
AIRFLOW_PARALLELISM=4
AIRFLOW_BATCH_SCHEDULE=
```

Không commit `.env`. Trên Azure, các giá trị nhạy cảm được quản lý trong Key Vault / Databricks Secret Scope thay vì lưu trong file plain-text.
Docker Compose tự nạp `.env`; config loader của các job local cũng đọc file này. Các target Terraform/Databricks
đọc `.env.cloud` riêng, vì vậy không cần nhân bản biến local trong Makefile.

Airflow local mặc định có bốn executor slots (`AIRFLOW_PARALLELISM=4`) để DAG local và cloud không chặn nhau khi
cloud operator đang poll Databricks. Mỗi Batch DAG vẫn có `max_active_runs=1` và `max_active_tasks=1`, nên năm stage
trong một DAG luôn chạy đúng thứ tự.

Cloud target dùng external Delta tables trong catalog `dbw_tk1_student_dev_sea`, với base schema names
`ecommerce_bronze`, `ecommerce_silver`, `ecommerce_gold`. Bundle tạo schema và task lấy tên trực tiếp từ schema
resources. Target `dev` tự thêm prefix theo user; target `prod` giữ base names. Parquet và `_delta_log` nằm dưới
`abfss://lakehouse@sttk1lakeheming01.dfs.core.windows.net/ecommerce-pipeline/<target>/`. External Location dùng
Access Connector managed identity, nên Spark không cần storage account key.

Không đặt password PostgreSQL vào bundle. Tạo Databricks secret scope với một secret:

```text
scope: ecommerce-pipeline
keys:
  postgres-password
```

`POSTGRES_PASSWORD` trong `.env` chỉ phục vụ generator và pipeline local; target `run-batch-cloud` không truyền
password này lên Databricks.

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

Vì reset dùng `TRUNCATE ... RESTART IDENTITY`, `make seed` sẽ fail-fast khi local Lakehouse hoặc checkpoint cũ
còn tồn tại. PostgreSQL CDC không sinh row-level `DELETE` cho truncate, nên giữ state cũ có thể để lại row mồ côi
trong Unified Silver. Muốn tạo baseline mới, chạy `make pg-reset` trước để PostgreSQL, Kafka, Lakehouse và cả hai
checkpoint cùng bắt đầu trong một source epoch. Dùng `make seed-stream` cho thay đổi incremental khi pipeline đang chạy.

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
3. Dựng candidate dimensions + `fact_sales` từ Unified Silver.
4. Chạy quality gate và source-to-Gold reconciliation.
5. Atomically publish release marker chứa Delta version của toàn bộ Gold tables; shared writer lock tuần tự hóa
   batch và streaming khi cả hai cùng yêu cầu cập nhật Gold.
6. Ghi structured log `[batch-run]` vào task log; không tạo file run JSON.

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

### Quản lý batch local bằng Airflow

Airflow là lớp orchestration; PostgreSQL extraction và mọi Spark/Delta transformation vẫn nằm trong package
`ecommerce_pipeline` hoặc Databricks. Local và cloud dùng chung một logical flow:

```text
check_source → run_bronze → run_silver → run_gold → validate_release
```

Mọi stage trong một DAG run dùng cùng `batch_id`. Local dùng `BashOperator` gọi entrypoint hiện có; cloud dùng
`DatabricksRunNowOperator` chính thức để trigger cùng Databricks Job với `pipeline_mode` tương ứng. Delta commit
metadata, pipeline progress và Gold release marker là source of truth giữa các process. XCom cloud chỉ chứa
Databricks run ID/URL nhỏ do provider quản lý, không truyền manifest, DataFrame hoặc business data.

Gold đã chạy quality gate trước khi publish. Vì vậy `validate_gold_release` trong critical path kiểm tra atomic
release marker và table contract, không full-scan lại toàn bộ Lakehouse. Deep validation vẫn dùng `make validate`
để chạy thủ công, sau migration hoặc theo một lịch kiểm tra riêng.

`check_source` kết nối read-only bằng psycopg, xác nhận database/user, quyền
`USAGE`/`SELECT` và contract cột của `customer_app.change_events`. Task
`run_bronze` khởi tạo SparkSession rồi dùng ngay session đó để ingest Bronze; không tạo
một Spark check process rồi tắt trước khi xử lý dữ liệu. Log task in `STARTING`, `RUNNING`, Spark version,
application ID và `spark_startup`.

Khởi động Airflow local cùng source PostgreSQL:

```bash
make airflow-up
make airflow-check
```

UI chỉ bind local tại `http://localhost:8080`. Runtime dùng metadata PostgreSQL riêng (`airflow-db`), không ghi bảng
Airflow vào source e-commerce. Image đã có Java 17, PySpark và các Delta/PostgreSQL JAR đã pin checksum; task không
phải tải Maven dependency lúc khởi động. Source/config được mount read-only, còn
`data/` và `logs/` dùng chung với lệnh batch chạy trực tiếp.
Compose này chỉ dành cho local: UI không yêu cầu đăng nhập và các internal signing key có default local. Khi đưa
orchestration lên cloud phải dùng secret manager/SSO và thay task bằng Databricks job trigger, không deploy nguyên
stack Compose này.

DAG mặc định không có lịch để tránh tự động chạy ngoài ý muốn. Trigger từ UI hoặc CLI:

```bash
make airflow-trigger
make airflow-status
make airflow-logs
```

Airflow local cũng có thể quản lý batch job trên Databricks, nhưng lúc đó nó chỉ đóng vai trò scheduler/monitor,
không chạy Spark trong container. Deploy cloud job trước bằng bundle, sau đó đặt các biến sau trong `.env`:

```dotenv
DATABRICKS_HOST=https://<workspace>.azuredatabricks.net
DATABRICKS_TOKEN=<personal-access-token-or-service-principal-token>
DATABRICKS_JOB_ID=<deployed-job-id>
```

Trigger cloud job từ Airflow local:

```bash
make airflow-trigger-cloud
```

DAG `ecommerce_databricks_batch_cloud` dùng cùng năm task như local:

```text
check_source → run_bronze → run_silver → run_gold → validate_release
```

Mỗi task trigger một Databricks run theo stage trên cùng existing cluster. Provider Airflow lưu run ID bền vững và
reconnect khi worker retry, thay cho custom HTTP polling. Stage retry-safe nhờ Bronze cursor, Delta idempotent
transactions, Silver progress và atomic Gold release. Việc tách process tăng task startup overhead, nhưng giữ
lỗi và thời gian từng layer hiển thị trực tiếp trên Airflow graph.

`DATABRICKS_HTTP_TIMEOUT_SECONDS` giới hạn thời gian một REST status request; timeout chỉ retry việc đọc
status của durable run ID, không submit run thứ hai. Không chạy `make deploy-batch-cloud` trước mỗi batch:
deploy tạo wheel artifact mới và Databricks phải cài/reconcile library ở run đầu tiên. Chỉ deploy khi code,
config hoặc job definition thay đổi; run hàng ngày dùng `make airflow-trigger-cloud`.

Sau khi thay đổi job parameters phải chạy `make deploy-batch-cloud` trước khi trigger DAG cloud.

Muốn đặt lịch, thêm cron vào `.env`, ví dụ chạy mỗi giờ:

```dotenv
AIRFLOW_BATCH_SCHEDULE=0 * * * *
```

Sau khi đổi schedule, chạy lại `make airflow-up`. DAG đặt `max_active_runs=1`; file/Delta lock vẫn bảo vệ khi có
CLI hoặc CDC chạy đồng thời. Batch DAG không phụ thuộc CDC Job, nên Debezium/CDC tiếp tục nhận và xử lý event độc lập.

Batch và Streaming đều dùng `unified_lakehouse_writer`: Silver và Gold acquire/release riêng theo production
semantics. Nếu manifest của Batch bị Streaming supersede giữa hai lock, Gold refresh live Silver progress dưới Gold
lock rồi hoàn tất idempotently; không dùng outer lock bao trọn Silver → Gold.

Tắt Airflow nhưng giữ metadata history:

```bash
make airflow-down
```

Local dùng POSIX advisory lock trên một file inode ổn định để ngăn hai writer chạy đồng thời:

```text
data/runtime/
└── _pipeline.lock               # file control tồn tại; kernel lock chỉ được giữ khi writer đang chạy
```

Pipeline không tạo `logs/batch_runs/*.json` ở local hoặc Azure. Trạng thái cuối được in thành một dòng JSON có
prefix `[batch-run]` trong task log. Payload vẫn có `timings_ms.spark_startup`, `bronze`, `silver`, `gold`, `total`
và timing chi tiết từng bước; không serialize danh sách `tables` hoặc `outputs` để log ngắn và dễ đọc.
Airflow/Databricks quản lý task state và retry; Databricks system tables và Azure Monitor dùng cho lịch sử vận hành
tập trung. Cursor/version phục vụ tính đúng dữ liệu vẫn nằm trong Delta commit metadata, không phụ thuộc file log.
Airflow task logs được lưu trong Docker named volume `airflow_logs`, không tạo thư mục log trong project trên laptop.

### Bước 6 — validate kết quả

```bash
make validate
```

Lệnh trả JSON gồm số version Bronze, số current rows Silver, số order/items/fact và tổng gross/discount/tax/shipping/net của Gold. Nếu có sai lệch lớn hơn `0.01`, job fail.

## 5. Chạy PostgreSQL CDC streaming ở local

CDC stack dùng PostgreSQL logical replication, Kafka KRaft và Debezium Connect. `cdc-up` không reset database;
nó chạy bootstrap idempotent để tạo replication role/publication trên cả database mới và volume đã tồn tại:

```bash
make cdc-up
make cdc-status
```

Connector chụp consistent initial snapshot của 12 bảng rồi tiếp tục đọc WAL. `change_events` không thuộc publication
và `app_users.password_hash` bị loại ngay tại connector. Debezium SMT gom event vào 6 data topics theo domain
`ecommerce.domain.<domain>`; `source.table` vẫn giữ tên bảng gốc để downstream vật lý hóa đủ 12 typed Bronze tables.
Spark đọc 6 topic bằng `subscribePattern`; Kafka Connect vẫn dùng ba compacted internal topics riêng.

Đọc hết Kafka event hiện có vào Raw CDC Bronze, sau đó merge backlog vào Unified Silver và cập nhật cùng một Gold:

```bash
make run-cdc-local-once
```

Hai bước cũng có thể chạy riêng để kiểm tra:

```bash
make run-stream-local-once
make run-silver-stream-local-once
```

Chạy liên tục ở hai terminal và tạo source changes ở terminal thứ ba:

```bash
make run-stream-local
```

```bash
make run-silver-stream-local
```

```bash
make seed-stream ORDERS_PER_BATCH=2 INTERVAL_SECONDS=3
```

Continuous downstream merge micro-batch vào Unified Silver, commit durable request vào
`silver/gold_reconcile_queue`, nhả shared writer lock rồi mới reconcile Gold ở control loop độc lập. Job
`reconcile-gold-local` vẫn dùng được để backfill hoặc repair thủ công từ Unified Silver đã hợp nhất:

```bash
make reconcile-gold-local
```

Output và recovery state:

```text
data/lakehouse/bronze/cdc_events/                 # raw append-only CDC envelope Delta
data/lakehouse/bronze/streaming/<table>/          # 12 typed append-only Delta tables
data/lakehouse/bronze/streaming/quarantine/       # invalid CDC side-output
data/checkpoints/ecommerce-cdc-to-bronze/v3/     # Kafka offsets + query metadata
data/lakehouse/silver/<table>/                    # shared Batch + CDC current state
data/lakehouse/gold/<table>/                      # shared curated model
data/checkpoints/ecommerce-cdc-to-silver/v3/      # raw Delta source progress + admission control
```

Bronze giữ nguyên `key_json`, `value_json`, Kafka topic/partition/offset, PostgreSQL LSN/transaction và cờ parse
validation. `_transport_event_id = topic:partition:offset` là transport identity. Query dùng một Delta sink và một
checkpoint riêng, vì vậy restart tiếp tục từ offset đã commit.

Downstream query đọc Raw CDC Delta như append stream và dùng một `foreachBatch`. Nó chọn `after` cho
snapshot/create/update, chọn `before` cho delete, kiểm tra schema drift/primary key và cast timestamp, decimal,
boolean theo contract. Nó tạo đủ 12 Delta table typed ở `bronze/streaming/<table>`, append valid rows bằng idempotent
transaction, rồi đọc lại đúng `_batch_id` đã commit làm input cho Silver. Trên Unity Catalog, tên metadata là
`cdc_typed_<table>` để không trùng Batch Bronze, còn external location vẫn giữ layout trên.
Nếu Silver stream khởi động trước Kafka-to-Bronze stream, job tự tạo Raw CDC Delta table rỗng từ chính normalized
Debezium schema; hai query vì vậy không phụ thuộc thứ tự startup.

Record không hợp lệ được append vào `bronze/streaming/quarantine` cùng raw envelope, lý do, thời điểm và batch ID.
Query log `[cdc-quality-alert]` rồi tiếp tục valid rows; việc replay cùng Spark batch ID không append quarantine trùng.

Mỗi table được transform bằng cùng Silver rule của batch rồi merge vào `data/lakehouse/silver/<table>`. Merge dùng
event time, ingestion priority và source sequence; CDC thắng batch khi cùng event time. Delete được lưu thành
tombstone nhưng giữ business context của target; delete thiếu before-image bắt buộc được quarantine. PostgreSQL dùng
`REPLICA IDENTITY FULL` cho cả 12 bảng để child delete vẫn có foreign key phục vụ Gold readiness. Transaction identity gồm query name, checkpoint version,
table name và Spark batch ID; retry bỏ qua transaction đã commit và tiếp tục table còn lại. Với local continuous query,
Gold đọc durable queue mỗi `streaming.silver.gold_reconcile_interval_seconds` và publish sau khi Silver thành công.
Nếu Silver chưa có đủ fact source từ các Kafka domain topics, request vẫn ở trạng thái `pending`; control loop log
`[gold-reconcile] status=deferred reason=source_fact_incomplete` rồi thử lại ở chu kỳ kế tiếp mà không giữ Spark
`foreachBatch`. Khi source đã đủ, Gold publish và queue được đánh dấu `published`. Nếu quality vẫn fail, stream log
`[gold-quality-alert]`, giữ request pending và tiếp tục xử lý.

Batch và CDC-to-Silver chỉ giữ lock trong lúc ghi Shared Silver/Gold. Local dùng `_pipeline.lock`; cloud dùng bảng
Delta `_pipeline_writer_locks` với optimistic concurrency và TTL, nên khóa có hiệu lực giữa hai Databricks jobs khác
nhau. `max_concurrent_runs: 1` vẫn được giữ để chống trùng run trong cùng job, nhưng không được coi là cross-job lock.
Batch và streaming dùng chung lock name; sau khi lấy lock, `GoldBuilder` đọc lại Silver/Gold release versions rồi mới
quyết định incremental build, full bootstrap hoặc no-op.

Tắt broker/connector nhưng giữ volume và offset:

```bash
make cdc-down
```

Không xóa riêng Bronze hoặc checkpoint. `make pg-reset` tạo source epoch mới nên tự động xóa toàn bộ generated
lakehouse và checkpoint sau khi reset PostgreSQL; nếu giữ Batch Bronze cursor hoặc streaming offset cũ, pipeline có
thể bỏ qua event. Batch extractor cũng fail-fast khi `change_events.event_id` lùi so với Bronze cursor. Sau reset,
seed source, chạy `make cdc-up`, rồi bootstrap lại pipeline.

Nếu Silver hiện có còn schema cũ, dừng streaming, chạy `make unified-state-reset`, bootstrap lại bằng
`make run-batch-local`, rồi chạy `make run-silver-stream-local-once` để replay Raw CDC. Target reset này chỉ xóa
generated Silver/Gold và checkpoint downstream; Batch Bronze và Raw CDC Bronze vẫn được giữ.

## 6. Chứng minh incremental và idempotency

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

Thay đổi customer, product, shop hoặc category tạo SCD2 version mới; nhiều thay đổi trên cùng entity giữa hai Gold
runs vẫn giữ đủ intermediate versions. Fact cũ giữ nguyên dimension version tại `order_created_at`. Giá/tồn kho
variant được cập nhật Type 1. Order bị hard delete biến mất khỏi Silver current-state và các fact tương ứng cũng bị
xóa khỏi Gold.

Có thể chạy toàn bộ demo trên bằng:

```bash
make demo-batch-local
```

## 7. Chạy từng layer khi phát triển

```bash
.venv/bin/python -m ecommerce_pipeline.jobs.run_batch --env local --mode bronze
.venv/bin/python -m ecommerce_pipeline.jobs.run_batch --env local --mode silver
.venv/bin/python -m ecommerce_pipeline.jobs.run_batch --env local --mode gold  # bị chặn khi streaming owns Gold
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

Gold cũng có explicit full rebuild cho schema/business-rule migration. Cần dừng các trigger tự động trong lúc migration
để tránh một incremental run nối tiếp ngay sau rebuild:

```bash
.venv/bin/python -m ecommerce_pipeline.jobs.run_batch \
  --env local --mode gold --full-rebuild-gold
```

Sau khi nâng schema Gold để thêm SCD2 cho product/shop/category, cần chạy full rebuild Gold đúng một lần.

## 8. Test và quality gates

Kiểm tra định dạng mã nguồn, static analysis, type checking và 176 unit tests:

```bash
make format-check
make lint
make type-check
make test
```

Kiểm tra hợp đồng CDC PostgreSQL (Replication Role, Publication 12 bảng + Heartbeat, REPLICA IDENTITY FULL):

```bash
make pg-up
make pg-wait
make test-integration
```

Kiểm tra các kịch bản End-to-End (E2E) như trong GitHub Actions CI:

```bash
make test-e2e-batch           # Luồng Batch toàn trình PostgreSQL → Bronze → Silver → Gold
make test-e2e-streaming       # Luồng CDC Debezium → Typed Bronze → Unified Silver → Gold queue
make test-e2e-concurrency     # Race condition: Shared Writer Lock giữa Batch và CDC
```

Lệnh kiểm tra nhanh toàn bộ chất lượng mã nguồn (Quality Gate):

```bash
make check
```

## 9. Incremental không dùng checkpoint tự quản

Mỗi bảng có progress độc lập nhưng progress nằm hoàn toàn trong Delta log:

```text
PostgreSQL → Bronze:
  Bronze commitInfo.userMetadata.last_event_id
  BronzeBatchManifest:
    batch_id, table_name, record/operation counts,
    committed Delta version, schema version

Bronze → Silver:
  Silver commitInfo.userMetadata.last_processed_bronze_version
  Silver commitInfo.userMetadata.silver_schema_version
  + Bronze Change Data Feed [lastProcessedVersion + 1, latestVersion]
  SilverBatchManifest:
    table_name, committed versions, schema versions

Silver → Gold:
  fact_sales TBLPROPERTIES.pipeline.goldActiveRelease.{silver_versions, gold_versions}
  + Silver Change Data Feed cho từng source table

Gold write → Publisher:
  GoldCandidateManifest:
    changed tables, committed Gold versions, Silver versions,
    quality status, release batch_id
```

Run bình thường không scan full Bronze Parquet. Silver chỉ đọc các file change của những Delta version mới rồi
`MERGE`. Gold lấy affected IDs từ Silver CDF; payment/shipment/voucher changes được lan truyền về đúng `order_id`,
sau đó chỉ các fact rows thuộc order bị ảnh hưởng được dựng lại. Product/shop/category changes chỉ mở SCD2 version
mới và không rewrite historical facts.

Bronze, Silver và các Gold dimension độc lập dùng Spark FAIR scheduling, tối đa `spark.max_parallel_tables` table
cùng lúc. Fact chỉ chạy sau khi dimension hoàn tất. Mặc định là 4; giảm giá trị này nếu PostgreSQL hoặc cluster bị
giới hạn connection/CPU.

Chỉ lần tạo Silver đầu tiên và `--full-rebuild-silver` đọc full Bronze snapshot. Bảng Bronze/Silver đã tồn tại nhưng
thiếu progress metadata được xem là state không hợp lệ; pipeline fail rõ ràng để reset layer hoặc chạy explicit rebuild,
không có nhánh tự migrate/full-scan dữ liệu cũ. Gold đọc full Silver đúng một lần khi chưa có release, hoặc khi chạy
`--full-rebuild-gold`.

Gold progress và publish state dùng chung một release marker, không có checkpoint file hay Delta table điều phối
riêng. Candidate tables được ghi trước; sau khi quality gate pass, một metadata-only `ALTER TABLE SET TBLPROPERTIES`
commit ghi marker vào `_delta_log` của `fact_sales`. Publisher tái sử dụng version từ release trước cho bảng không
đổi và chỉ đọc latest history của các Gold table vừa được ghi. Marker vẫn chứa version chính xác của cả 11 Gold
tables. Đọc marker từ current Delta metadata không scan Parquet và cũng không scan toàn bộ history. Consumer chụp
marker một lần rồi đọc mọi table bằng `versionAsOf`: trước commit thấy toàn bộ release cũ, sau commit thấy toàn bộ
release mới.

Nếu job lỗi giữa chừng, marker cũ giữ nguyên nên dữ liệu candidate chưa hoàn tất không được publish. Retry đọc
lại cùng Silver CDF range và các Delta merge/delete theo key vẫn idempotent. Trong code Python, entrypoint đọc
analytics-ready Gold là:

```python
snapshot = GoldReleaseStore(spark, config).snapshot()
fact_sales = snapshot.read_table("gold", "fact_sales")
dim_product = snapshot.read_table("gold", "dim_product")
```

Không dùng `spark.read.load(.../gold/...)` trực tiếp cho consumer vì cách đó đọc physical latest version, bao gồm
cả candidate chưa publish. Gold release chỉ dùng marker native trong `fact_sales/_delta_log`; pipeline không tạo
thêm bảng điều phối.

## 10. Chạy trên Azure Databricks

Yêu cầu:

- Databricks CLI đã đăng nhập bằng profile `ecommerce-dev`.
- Databricks workspace truy cập được Azure PostgreSQL qua network/firewall.
- Existing catalog `dbw_tk1_student_dev_sea` và quyền `USE CATALOG`, `CREATE SCHEMA` cho deployment identity.
- Unity Catalog storage credential riêng dùng Azure Databricks Access Connector; không dùng workspace default
  credential cho StorageV2 account bên ngoài.
- Managed identity của Access Connector có `Storage Blob Data Contributor` trên container dữ liệu và
  `Storage Blob Delegator` trên Storage Account.
- Secret scope `ecommerce-pipeline` có key `postgres-password`.
- `.env.cloud` có cấu hình kết nối PostgreSQL và các giá trị theo môi trường Databricks/Unity Catalog/Storage
  được liệt kê trong `.env.cloud.example`. `Makefile` chỉ kiểm tra rồi truyền các giá trị này vào bundle.
- Mật khẩu PostgreSQL không lưu trong `.env.cloud`; job đọc key `postgres-password` từ Databricks secret scope.

Tạo file cloud riêng để không ghi đè cấu hình PostgreSQL local:

```bash
cp .env.cloud.example .env.cloud
```

Tạo secret một lần (CLI sẽ yêu cầu nhập giá trị bí mật):

```bash
databricks secrets create-scope ecommerce-pipeline
databricks secrets put-secret ecommerce-pipeline postgres-password
```

Khi code, config hoặc định nghĩa Databricks Job thay đổi, deploy một lần:

```bash
make deploy-batch-cloud
```

Mỗi batch thường kỳ (bao gồm job do Airflow trigger) chỉ chạy artifact đã deploy:

```bash
make run-batch-cloud
```

Muốn deploy và chạy ngay trong một lệnh khi phát hành phiên bản mới:

```bash
make deploy-run-batch-cloud
```

Không deploy ở mỗi lịch Airflow: bundle dùng dynamic version, mỗi deploy tạo một wheel mới. Việc gắn nhiều wheel
lịch sử vào existing compute làm tăng thời gian `Installing libraries` và các artifact cũ đã bị dọn có thể khiến
task không bắt đầu được. So sánh thời gian xử lý bằng `timings_ms.total` trong dòng `[batch-run]`; wall-clock còn
bao gồm queue, khởi động compute và cài library.

Target này không chạy Spark trên laptop. Laptop chỉ đóng gói wheel và upload artifact; toàn bộ JDBC ingestion và
Bronze/Silver/Gold chạy trên existing compute cấu hình trong bundle
(`0804-071458-pswonf6z`). Bundle không tạo hoặc xóa compute. Dynamic wheel chỉ được tạo trong bước deploy; Airflow
và `make run-batch-cloud` chỉ trigger job đã deploy.
YAML chỉ tồn tại một bản trong `configs/`; bundle đồng bộ thư mục này lên workspace và truyền đường dẫn tuyệt đối
`--base-config`/`--env` cho wheel task.

Cloud target này dùng namespace Unity Catalog mới và deterministic external paths, không đọc thư mục ABFSS
`lakehousetest` cũ. Lần chạy đầu tạo external tables và nạp lại từ `change_events`; cần bảo đảm event history chưa
bị purge. Nếu cần giữ physical Delta history cũ, hãy migrate/register dữ liệu đó trước khi chuyển target.

Lệnh in URL của run. Mở URL đó để xem Spark UI, stdout/stderr, executor logs và stack trace trong lúc job chạy.
Console chỉ in summary ngắn theo layer và trạng thái batch; JSON đầy đủ gồm từng table, output, timings và error
được giữ tại
`/Workspace/Users/2251120184@ut.edu.vn/ecommerce-pipeline/logs` và vẫn còn sau khi compute terminate. Job có
`max_concurrent_runs: 1`; shared Silver còn dùng Delta-backed cloud lock để bảo vệ khi batch và CDC được triển khai
thành hai jobs khác nhau. Local pipeline dùng `logs/_pipeline.lock`. Log driver/executor chính được giữ trong
Databricks Job run. Muốn xem thêm log debug của Databricks CLI:

```bash
make run-batch-cloud DATABRICKS_FLAGS=--debug
```

Job đặt `max_concurrent_runs: 1` để giữ mô hình một writer của pipeline. Spark/Delta có sẵn trong Databricks Runtime,
vì vậy wheel cloud không đóng gói `pyspark` hoặc `delta-spark`; pipeline dùng PostgreSQL JDBC driver tích hợp trong
Runtime 16.4 thay vì cài thêm Maven library. Task lấy password PostgreSQL bằng `dbutils.secrets`; quyền ADLS đến từ
Unity Catalog managed identity/storage credential, không dùng account key trong code.

## 11. Cloud CDC tạm thời: Event Hubs + ACI Debezium bằng Terraform

Terraform trong `infra/cloud/cdc` chỉ quản lý tài nguyên có phí cần tạo/xóa theo phiên demo:

- Một Event Hubs Standard namespace, 1 throughput unit, 6 data hubs theo domain và 2 control hubs
  (heartbeat/transaction), retention 1 ngày. Tổng 8 hubs nằm dưới giới hạn 10 hubs/Standard namespace.
- Hai SAS policy tối thiểu: ACI chỉ có `Send`, Databricks chỉ có `Listen`.
- Một Azure Container Instance 1 vCPU/1.5 GiB chạy Debezium Server `3.6.0.Final`.

Resource group `rg-tk1-student-cdc-dev` phải tồn tại trước. Terraform không quản lý PostgreSQL, Databricks hoặc ADLS.
Debezium offset được giữ trong `cdc_control.debezium_offset_storage` trên PostgreSQL, nên destroy ACI/Event Hubs không
làm mất WAL position. Mỗi lần destroy/apply tạo Event Hubs namespace có suffix mới. Namespace này đồng thời là source
epoch trong `_transport_event_id` và checkpoint path trên ADLS; offset mới bắt đầu từ 0 không thể va chạm event/checkpoint
của namespace cũ. Chỉ checkpoint Event Hubs → Raw Bronze đổi theo epoch; checkpoint Raw Bronze → Silver giữ ổn định,
vì Delta Raw Bronze không bị destroy và không được replay toàn bộ sau mỗi lần apply.

Event Hubs Kafka endpoint yêu cầu Standard tier và `SASL_SSL`/`PLAIN`; cấu hình Spark sử dụng đúng protocol options theo
[Azure Event Hubs Kafka Spark](https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-kafka-spark-tutorial).
Debezium Server dùng Kafka sink và JDBC offset store theo
[Debezium Server documentation](https://debezium.io/documentation/reference/operations/debezium-server.html).
`ByLogicalTableRouter` gom topic vật lý thành `customer`, `catalog`, `promotion`, `sales`, `payment`, `shipping`;
Debezium envelope vẫn giữ `source.table`, vì vậy downstream vẫn vật lý hóa đúng 12 canonical typed Bronze tables.

### Chuẩn bị PostgreSQL và biến bí mật

Trên Azure PostgreSQL Flexible Server, đặt static server parameter `wal_level=logical`, lưu thay đổi rồi restart server
một lần. Điền thêm vào
`.env.cloud` (không commit):

```dotenv
POSTGRES_PASSWORD=<admin-password-used-only-by-bootstrap>
CDC_POSTGRES_PASSWORD=<dedicated-replication-role-password>
AZURE_SUBSCRIPTION_ID=<subscription-id>
AZURE_CDC_RESOURCE_GROUP=rg-tk1-student-cdc-dev
AZURE_CDC_NAME_PREFIX=tk1-ecommerce-cdc-dev
```

PostgreSQL firewall/network phải cho phép kết nối từ ACI tới cổng 5432. Cấu hình tiết kiệm hiện tại không tạo VNet,
NAT Gateway hoặc public inbound IP cho ACI; vì vậy nếu PostgreSQL không cho phép kết nối từ Azure services thì cần thêm
firewall rule phù hợp trước khi apply. Không đưa PostgreSQL firewall vào Terraform này vì PostgreSQL nằm ngoài phạm vi
tài nguyên được phép quản lý/xóa.

Bootstrap idempotent sẽ tạo/cập nhật role `ecommerce_cdc`, publication 12 bảng, xác nhận cả 12 bảng dùng
`REPLICA IDENTITY FULL`, và tạo schema control riêng cho offset/heartbeat. Debezium cập nhật heartbeat row mỗi 10 giây;
health gate đọc tuổi heartbeat và WAL retained bytes trực tiếp từ PostgreSQL thay vì suy đoán từ Spark. Password PostgreSQL
và Event Hubs connection string đi qua Terraform sensitive variables/ACI secure environment variables; local Terraform
state vẫn chứa secret nên `.terraform`, `*.tfstate` và `terraform.tfvars` đã bị git-ignore.

### Rollout production an toàn, chạy và destroy

Xem plan trước khi phát sinh chi phí:

```bash
make cdc-cloud-plan
```

Chạy toàn bộ gate đến concurrency canary. Target này deploy job ở trạng thái `PAUSED` trước, bootstrap PostgreSQL,
apply Event Hubs/ACI, health check, chạy `availableNow` canary và cuối cùng chạy Batch+CDC đồng thời. Nếu bất kỳ gate
nào lỗi, Make dừng ngay và Continuous Job vẫn `PAUSED`; Delta checkpoint và Debezium offset không bị reset:

```bash
make cdc-cloud-rollout
```

Có thể chạy/điều tra từng gate độc lập:

```bash
make deploy-cdc-cloud-paused       # code/job definition, chưa tạo tài nguyên CDC
make cdc-cloud-pg-bootstrap        # role/publication/identity/heartbeat
make cdc-cloud-apply               # IaC + secret + deploy namespace thật; vẫn PAUSED
make cdc-cloud-health              # ACI, slot, heartbeat age, WAL retained bytes
make cdc-cloud-canary              # availableNow + deep Raw/Typed/Silver/Gold validation
make cdc-cloud-concurrency-canary  # Batch và CDC thật chạy đồng thời, rồi validate converge
```

`make run-cdc-cloud` là một `availableNow` cycle hữu hạn; `make cdc-cloud-canary` bật thêm deep validation. Canary kiểm
tra table contract/existence, duplicate Raw/Typed/Silver, Gold release, freshness,
quarantine và durable pending-Gold queue.

CDC job dùng cùng existing cluster `0804-071458-pswonf6z` với batch. Không tạo compute thứ hai. Production path là một
Databricks Continuous Job mặc định paused; trong một task dài hạn, Raw và Unified Silver queries chạy đồng thời với
processing-time trigger 5 giây. Cách này loại bỏ cold start và library installation khỏi từng micro-batch:

```bash
make cdc-cloud-start
make cdc-cloud-stop
```

Chỉ bật sau khi rollout báo `CANARIES_PASSED`. Theo dõi ít nhất ba micro-batches thực sự có `input_rows > 0`; nếu query
lỗi hoặc freshness/lag tăng, chạy `make cdc-cloud-stop`, rollback wheel/job definition và chạy lại canary. Không xóa
`_checkpoints`, replication slot hoặc JDBC offset table trong rollback.

Các structured metric/alert có thể tìm trực tiếp trong Databricks task logs:

- `[cdc-runtime-metrics]`: input rows, Event Hubs offsets behind latest, micro-batch duration và stage duration.
- `[cdc-health]`: Debezium heartbeat age, PostgreSQL WAL retained bytes và slot state.
- `[cdc-canary]`: Raw/Typed/Silver/Gold freshness, pending Gold age/count và quarantine count.
- `[pipeline-lock]`: lock wait, timeout và held duration của `unified_lakehouse_writer`.
- `[cdc-alert]`: lag/duration threshold, pending/quarantine, cycle failure hoặc Gold publish failure.

Continuous Job giữ cluster sống nên phát sinh DBU/VM liên tục. Với tài khoản EDU, ưu tiên `make run-cdc-cloud` thủ công
nếu chỉ cần minute-level latency, hoặc chỉ unpause Continuous Job trong cửa sổ test. Batch và CDC dùng chung Delta writer lock để không publish Silver/Gold đồng
thời; Spark FAIR scheduler giảm starvation nếu hai job tình cờ overlap, nhưng single-node 4-core vẫn chậm hơn khi chạy
đồng thời.

Khi hoàn tất, lệnh destroy pause Continuous Job trước rồi chỉ xóa Event Hubs và ACI:

```bash
make cdc-cloud-destroy
```

PostgreSQL role/publication/replication slot và JDBC offset table được giữ lại để lần apply sau tiếp tục từ WAL position.
Nếu muốn xóa hoàn toàn CDC state, phải thực hiện một thao tác PostgreSQL riêng có chủ đích; Terraform không tự xóa state
nguồn nhằm tránh mất dữ liệu.

Replication slot tiếp tục giữ WAL nếu PostgreSQL vẫn chạy và có write trong lúc ACI bị destroy. Sau khi destroy CDC,
hãy stop PostgreSQL như kế hoạch EDU hoặc theo dõi `pg_replication_slots`/dung lượng WAL. Chỉ drop slot khi chấp nhận mất
continuity và sẽ chạy snapshot/recovery ở lần apply kế tiếp.

## 12. Giới hạn có chủ đích của batch JDBC

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

## 13. Phân tích hiệu năng thực tế trên Databricks Single-Node Cluster

Khi triển khai trên gói tiết kiệm chi phí (Azure Student / EDU) với cấu hình **Single-Node Cluster (`Standard_D4s_v4`: 4 vCPUs, 16GB RAM, 0 Workers)**:

### Chỉ số đo lường thực tế (Logs đo từ Databricks Job Run)
* **Tổng thời gian Batch Run:** ~ 4 phút (~ 249 giây).
* **Bóc tách thời gian từng tầng:**
  - **Bronze Extraction (12 bảng JDBC):** `18.27 giây` (~ 1.5s / bảng).
  - **Silver Merge (12 bảng Delta):** `73.19 giây` (~ 6s / bảng).
  - **Gold Dimensional Model (11 bảng DWH + SCD2 Replay):** `157.20 giây` (~ 2.6 phút). Trong đó: `dim_customer` (45.7s), `dim_category` (41.3s), `dim_product` (39.9s), `dim_shop` (27.1s), `fact_sales` (56.9s).

### Phân tích điểm nghẽn (Bottleneck Analysis)
1. **Azure Blob Storage (ADLS Gen2) REST API Overhead:** Khi ghi tuần tự 23 bảng Delta (`abfss://`), mỗi transaction yêu cầu hàng chục lệnh gọi REST API qua mạng để đọc metadata, ghi Parquet và commit `_delta_log`. Overhead cố định của Storage I/O chiếm tới 60-70% tổng thời gian chạy.
2. **SCD2 Full History Recompute trên 4 vCPUs:** Các bảng SCD2 Type 2 tính toán lại toàn bộ lịch sử với các hàm Window Function và SHA-256 trên một máy ảo duy nhất không có worker song song.
3. **Delta Transactions trên bảng không có thay đổi:** Các bảng không có bản ghi mới vẫn thực hiện Delta MERGE transaction xuống storage.

## 14. Troubleshooting

**Thiếu biến môi trường**: loader fail với `Missing required environment variable`. Tạo `.env` từ `.env.example`.

**Port 5432 đang được dùng**: đổi `POSTGRES_PORT` trong `.env`, sau đó chạy lại `make pg-reset`.

**Spark local không tải được JAR**: lần đầu chạy local cần internet để tải Delta Lake và PostgreSQL JDBC artifacts.
Kiểm tra proxy/firewall của Maven Central. Databricks Runtime 16.4 dùng driver tích hợp và không thực hiện bước này.

**Spark streaming warning quá nhiễu ở local**: local Spark dùng `infra/local/spark/log4j2.properties` để lọc warning
Kafka `AdminClientConfig` không có giá trị hành động. Local CDC ưu tiên near realtime: Raw Bronze trigger mỗi 2 giây,
downstream Silver/Gold trigger mỗi 5 giây. Gold readiness chỉ kiểm tra các `order_id` bị micro-batch hiện tại hoặc
batch trước đó ảnh hưởng, nên không scan toàn bộ Silver trong vòng polling bình thường.

**`run-silver-stream-local` cứ `input_rows=0` dù Kafka/PostgreSQL có data**: kiểm tra có ai đã xóa
`data/lakehouse/bronze/cdc_events` trong khi checkpoint `data/checkpoints/ecommerce-cdc-to-bronze/...`
vẫn còn không. Khi checkpoint Kafka đã advance nhưng Raw Bronze Delta mất, downstream chỉ thấy source rỗng. Dừng cả hai
stream, chạy `make cdc-state-reset`, rồi start lại `make run-stream-local` và `make run-silver-stream-local`.

**Debezium task FAILED với `LSN ... no longer available`**: Kafka Connect đang giữ offset cũ nhưng PostgreSQL không
còn WAL/schema history tại LSN đó, thường xảy ra sau khi reset PostgreSQL hoặc để connector dừng quá lâu. Reset offset
của connector rồi để Debezium snapshot lại:

```bash
make cdc-recover-offsets
make cdc-status
```

**Muốn chạy lại sạch Lakehouse nhưng giữ PostgreSQL**:

```bash
make lakehouse-reset
make run-batch-local
```

**Pipeline báo đang có writer khác**: kiểm tra job Spark/Batch/Airflow khác thực sự còn chạy. Không cần xóa
`data/runtime/_pipeline.lock`; POSIX lock được kernel tự nhả kể cả khi process bị kill cứng.
