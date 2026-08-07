# E-commerce Lakehouse Pipeline — Local và Azure Databricks

Repo này chạy batch pipeline trên Spark local/Azure Databricks và PostgreSQL CDC streaming ở local:

```text
                         PostgreSQL OLTP
                         /             \
          JDBC full snapshot          WAL / Debezium / Kafka
                   │                           │
                   ▼                           ▼
       Bronze Batch Raw              Bronze Streaming Raw CDC
       bronze/batch/<table>           bronze/streaming/cdc_events
                   \                 │
                    \       12 Typed Bronze + quarantine
                     \      bronze/streaming/<table>
                      \               /
                     ▼                       ▼
                  Unified Silver (một row hiện tại/PK + tombstone)
                                  │
                                  ▼
                  Unified Gold (star schema + fact_sales)
                                  │
                                  ▼
                       Analytics-ready Lakehouse
```

Spark/JDBC compute có thể chạy local hoặc trên Databricks. Local dùng filesystem; cloud dùng external Delta tables
trên StorageV2/ADLS Gen2 do project sở hữu. Unity Catalog quản lý metadata, permissions, lineage và governance.
Power BI có thể đọc Gold qua Databricks SQL. Local CDC dùng đúng Kafka protocol để khi triển khai cloud chỉ cần
đổi broker/security/checkpoint/storage configuration sang Event Hubs, ADLS và Unity Catalog. ADF và cloud streaming
deployment chưa nằm trong phạm vi hiện tại.

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
- Shared Silver writer dùng local file lock hoặc Delta-backed cloud lock theo từng micro-batch. Gold có đúng một owner
  cấu hình bằng `coordination.gold_owner`; local giao ownership cho streaming nên batch dừng ở Silver.
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

Nội dung mặc định cho local:

```dotenv
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=ecommerce
POSTGRES_USER=admin
POSTGRES_PASSWORD=admin
```

Không commit `.env`. Trên Azure, các giá trị này sẽ được inject từ Key Vault/secret scope thay vì sửa code.

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
3. Nếu batch là Gold owner, dựng candidate dimensions + `fact_sales`; local mặc định bỏ qua vì streaming là owner.
4. Khi batch là Gold owner, chạy quality gate và source-to-Gold reconciliation.
5. Khi batch là Gold owner, atomically publish release marker chứa Delta version của toàn bộ Gold tables.
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
không phải suy đoán từ số record. Chi tiết từng bảng nằm ở `bronze.table.<table>`, `silver.table.<table>` và
`gold.table.<table>`; các timing đã hoàn tất vẫn được giữ nếu batch lỗi.

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
và `app_users.password_hash` bị loại ngay tại connector. Mỗi bảng nguồn được ghi vào một Kafka topic riêng theo mẫu
`ecommerce.customer_app.<table>`; Spark đọc 12 topic này bằng `subscribePattern` rồi landing vào Raw CDC Bronze.
Kafka Connect vẫn dùng ba compacted internal topics riêng.

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

Continuous downstream merge micro-batch vào Unified Silver rồi reconcile Gold ngay khi
`streaming.silver.reconcile_gold_each_batch: true`. Job `reconcile-gold-local` vẫn dùng được để backfill hoặc repair
thủ công từ Unified Silver đã hợp nhất:

```bash
make reconcile-gold-local
```

Output và recovery state:

```text
data/lakehouse/bronze/streaming/cdc_events/       # raw append-only Delta
data/lakehouse/bronze/streaming/<table>/          # 12 typed append-only Delta tables
data/lakehouse/bronze/streaming/quarantine/       # invalid CDC side-output
data/checkpoints/ecommerce-cdc-to-bronze/v2/     # Kafka offsets + query metadata
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
tombstone để sự kiện batch cũ không resurrect dữ liệu. Transaction identity gồm query name, checkpoint version,
table name và Spark batch ID; retry bỏ qua transaction đã commit và tiếp tục table còn lại. Với local continuous query,
Gold đọc Delta CDF và publish sau từng micro-batch Silver thành công. Nếu Silver chưa có đủ fact source từ 12 Kafka
topics, stream log `[gold-reconcile] status=deferred reason=source_fact_incomplete`, retry trong cửa sổ
`streaming.silver.gold_deferred_retry_seconds`, rồi tiếp tục idle-reconcile định kỳ khi query không nhận thêm raw file;
khi source đã đủ thì pending scope được publish. Nếu quality vẫn fail, stream log `[gold-quality-alert]`, bỏ publish lần
đó và tiếp tục xử lý.

Batch và CDC-to-Silver chỉ giữ lock trong lúc ghi Shared Silver/Gold. Local dùng `_pipeline.lock`; cloud dùng bảng
Delta `_pipeline_writer_locks` với optimistic concurrency và TTL, nên khóa có hiệu lực giữa hai Databricks jobs khác
nhau. `max_concurrent_runs: 1` vẫn được giữ để chống trùng run trong cùng job, nhưng không được coi là cross-job lock.
`coordination.gold_owner` chặn writer Gold không phải owner ngay trong code.

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

Thay đổi customer, product, shop hoặc category tạo SCD2 version mới; fact cũ giữ nguyên dimension version tại
`order_created_at`. Giá/tồn kho variant được cập nhật Type 1. Order bị hard delete biến mất khỏi Silver current-state
và các fact tương ứng cũng bị xóa khỏi Gold.

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

Gold cũng có explicit full rebuild cho schema/business-rule migration khi cấu hình `gold_owner: batch`:

```bash
.venv/bin/python -m ecommerce_pipeline.jobs.run_batch \
  --env local --mode gold --full-rebuild-gold
```

Sau khi nâng schema Gold để thêm SCD2 cho product/shop/category, cần chạy full rebuild Gold đúng một lần.

## 8. Test và quality gates

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

## 9. Incremental không dùng checkpoint tự quản

Mỗi bảng có progress độc lập nhưng progress nằm hoàn toàn trong Delta log:

```text
PostgreSQL → Bronze:
  Bronze commitInfo.userMetadata.last_event_id
  BronzeBatchManifest:
    batch_id, changed_tables, record/operation counts,
    event_id ranges, previous/committed versions, schema versions

Bronze → Silver:
  Silver commitInfo.userMetadata.last_processed_bronze_version
  Silver commitInfo.userMetadata.silver_schema_version
  + Bronze Change Data Feed [lastProcessedVersion + 1, latestVersion]
  SilverBatchManifest:
    batch_id, changed_tables, processed Bronze CDF ranges,
    committed versions, schema versions, propagated source counts

Silver → Gold:
  fact_sales TBLPROPERTIES.pipeline.goldActiveRelease.{silver_versions, gold_versions}
  + Silver Change Data Feed cho từng source table

Gold write → Publisher:
  GoldCandidateManifest:
    changed tables, previous/committed Gold versions,
    quality status, release batch_id
```

Run bình thường không scan full Bronze Parquet. Silver chỉ đọc các file change của những Delta version mới rồi
`MERGE`. Gold lấy affected IDs từ Silver CDF; payment/shipment/voucher changes được lan truyền về đúng `order_id`,
sau đó chỉ các fact rows thuộc order bị ảnh hưởng được dựng lại. Product/shop/category changes chỉ mở SCD2 version
mới và không rewrite historical facts.

Bronze, Silver và các Gold dimension độc lập dùng Spark FAIR scheduling, tối đa `spark.max_parallel_tables` table
cùng lúc. Fact chỉ chạy sau khi dimension hoàn tất. Mặc định là 4; giảm giá trị này nếu PostgreSQL hoặc cluster bị
giới hạn connection/CPU.

Chỉ lần tạo Silver đầu tiên, lần tự migrate bảng Silver cũ chưa có progress, và `--full-rebuild-silver` đọc full
Bronze snapshot. Bronze cũ được migrate cursor tự động bằng cách đọc `MAX(_event_id)` đúng một lần và ghi marker
vào Delta log. Gold đọc full Silver đúng một lần khi chưa có progress, hoặc khi chạy `--full-rebuild-gold`.

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

Validate bundle, build wheel, deploy job rồi chạy và chờ kết quả:

```bash
make run-batch-cloud
```

Target này không chạy Spark trên laptop. Laptop chỉ đóng gói wheel và upload artifact; toàn bộ JDBC ingestion và
Bronze/Silver/Gold chạy trên existing compute `ecommerce-lakehouse-dev`
(`0729-012731-0heucyp7`). Bundle không tạo hoặc xóa compute. Mỗi deploy tạo dynamic wheel version để existing
compute không tái sử dụng package cũ có cùng version.
YAML chỉ tồn tại một bản trong `configs/`; bundle đồng bộ thư mục này lên workspace và truyền đường dẫn tuyệt đối
`--base-config`/`--env` cho wheel task.

Cloud target này dùng namespace Unity Catalog mới và deterministic external paths, không đọc thư mục ABFSS
`lakehousetest` cũ. Lần chạy đầu tạo external tables và nạp lại từ `change_events`; cần bảo đảm event history chưa
bị purge. Nếu cần giữ physical Delta history cũ, hãy migrate/register dữ liệu đó trước khi chuyển target.

Lệnh in URL của run. Mở URL đó để xem Spark UI, stdout/stderr, executor logs và stack trace trong lúc job chạy.
Console chỉ in summary ngắn theo layer và trạng thái batch; JSON đầy đủ gồm từng table, output, timings và error
được giữ tại
`/Workspace/Users/2251120184@ut.edu.vn/ecommerce-pipeline/logs` và vẫn còn sau khi compute terminate. Databricks
không dùng file lock vì Job đã đặt `max_concurrent_runs: 1`; local pipeline vẫn dùng `logs/_pipeline.lock`. Log
driver/executor chính được giữ trong Databricks Job run. Muốn xem thêm log debug của Databricks CLI:

```bash
make run-batch-cloud DATABRICKS_FLAGS=--debug
```

Job đặt `max_concurrent_runs: 1` để giữ mô hình một writer của pipeline. Spark/Delta có sẵn trong Databricks Runtime,
vì vậy wheel cloud không đóng gói `pyspark` hoặc `delta-spark`; pipeline dùng PostgreSQL JDBC driver tích hợp trong
Runtime 16.4 thay vì cài thêm Maven library. Task lấy password PostgreSQL bằng `dbutils.secrets`; quyền ADLS đến từ
Unity Catalog managed identity/storage credential, không dùng account key trong code.

## 11. Giới hạn có chủ đích của batch JDBC

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

## 12. Troubleshooting

**Thiếu biến môi trường**: loader fail với `Missing required environment variable`. Tạo `.env` từ `.env.example`.

**Port 5432 đang được dùng**: đổi `POSTGRES_PORT` trong `.env`, sau đó chạy lại `make pg-reset`.

**Spark local không tải được JAR**: lần đầu chạy local cần internet để tải Delta Lake và PostgreSQL JDBC artifacts.
Kiểm tra proxy/firewall của Maven Central. Databricks Runtime 16.4 dùng driver tích hợp và không thực hiện bước này.

**Spark streaming warning quá nhiễu ở local**: local Spark dùng `infra/local/spark/log4j2.properties` để lọc warning
Kafka `AdminClientConfig` không có giá trị hành động. Local CDC ưu tiên near realtime: Raw Bronze trigger mỗi 2 giây,
downstream Silver/Gold trigger mỗi 5 giây. Gold readiness chỉ kiểm tra các `order_id` bị micro-batch hiện tại hoặc
batch trước đó ảnh hưởng, nên không scan toàn bộ Silver trong vòng polling bình thường.

**`run-silver-stream-local` cứ `input_rows=0` dù Kafka/PostgreSQL có data**: kiểm tra có ai đã xóa
`data/lakehouse/bronze/streaming/cdc_events` trong khi checkpoint `data/checkpoints/ecommerce-cdc-to-bronze/...`
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

**Pipeline báo đang có writer khác**: đảm bảo không còn job Spark chạy. Nếu job trước bị kill cứng, xóa file generated `logs/_pipeline.lock` rồi chạy lại.
