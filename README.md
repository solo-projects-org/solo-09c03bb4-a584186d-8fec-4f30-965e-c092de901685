# 渡轮结算 · Iceberg 字段 ID 回填

湖仓值班员恢复渡轮结算的历史分区时，旧的 Parquet 副本往往还没有 Iceberg
字段 ID，查询引擎按 ID 读票价时就会找不到列。本工具接收**现有格式的有效文件
清单**、清单中 `data` 文件的 **Parquet 副本**，以及表属性
`schema.name-mapping.default` 的 **JSON 内容**，先给副本补上字段 ID，再按原
上传文件名打包成 ZIP。

补齐后的副本可以继续配合**原清单和位置删除文件**使用：只重写 `data` 文件，清
单与位置删除副本不参与打包。

## 快速启动

```bash
docker compose up --build
```

打开 <http://localhost:8000>。首页就是字段 ID 回填。点 **“载入样例”** 可以
直接看到内置演示。健康检查：

```bash
curl http://localhost:8000/healthz
```

停止：

```bash
docker compose down
```

> 另保留了原先的 **票号位置删除核对** 页面（`/checker` 或 `/sample`），见文末。

## 网页用法

1. 上传 **有效文件清单**（CSV 或 JSON）。
2. 上传清单中 `data` 文件的 **Parquet 副本**，可多选；文件名必须与清单的
   `uploaded_filename` 一致。
3. 粘贴 `schema.name-mapping.default` 的 JSON，或上传同名 JSON 文件。
4. 点击 **“补齐并打包 ZIP”**。整批成功后才返回 ZIP；任一步失败都只显示错误。

上传文件只保存在一次请求的临时目录中，请求结束即删除；不会用用户文件名拼接
服务器路径。

### 清单与 name mapping 格式

清单与原有核对工具完全一致：

```csv
original_path,uploaded_filename,file_type,data_sequence_number
s3://ferry-lake/warehouse/settlement/voyage_date=2026-11-05/data/data-0001.parquet,data-0001.parquet,data,7
s3://ferry-lake/warehouse/settlement/voyage_date=2026-11-05/delete-pos/delete-pos-0001.parquet,delete-pos-0001.parquet,position_delete,12
```

只重写 `data` 条目；`position_delete` / `equality_delete` 条目会被识别但不
重写，也不要求上传副本（上传了也只会被忽略）。清单列名别名、JSON 清单形式见
原核对说明。

name mapping 是 Iceberg 表属性里的字段数组：

```json
[
  {"field-id": 1, "names": ["ticket_no"]},
  {
    "field-id": 3,
    "names": ["base_fare", "legacy_base_fare"],
    "fields": [
      {"field-id": 31, "names": ["fare_cents", "amount_cents"]},
      {"field-id": 32, "names": ["currency"]}
    ]
  },
  {
    "field-id": 4,
    "names": ["surcharge"],
    "fields": [
      {"field-id": 41, "names": ["fare_cents"]},
      {"field-id": 42, "names": ["currency"]}
    ]
  }
]
```

顶层也可以是包含 `fields` / `mapping` 数组的对象。字段 ID 键接受
`field-id` / `field_id` / `id`，名称键接受 `names` / `name` / `aliases`。

## 补齐规则

- **只处理标量列和一层 struct**。list、map、union、struct 里再套 struct 等
  更深的嵌套会直接报错，并指出列路径。
- **已有 ID 不动**：列上已经有 `PARQUET:field_id` 时原样保留，不会被
  name mapping 覆盖。
- **缺 ID 的字段按别名唯一匹配**：列名必须恰好命中一个 name mapping 条目；
  一个都没命中或有多个命中都会中止。
- **子列先用父字段的 ID 找到对应的子映射**：struct 子列不在顶层查找，只在
  “父字段现有/新补的 ID → 对应映射条目” 的 `fields` 里查找。因此两个不同
  struct 下同名的旧金额列（例如 `base_fare.fare_cents` 与
  `surcharge.fare_cents`）会各自拿到父子映射里不同的 ID。
- **单文件内 ID 唯一**：已有 ID 与新补 ID 合在一起检查；同一文件里重复即
  报错，并列出冲突的两列。
- **分批重写**：按行批次读出、写入，列名、类型、值与行序保持不变；行数会
  在写完后复核，原压缩方式尽量沿用。
- **全有或全无**：缺文件、列无法唯一匹配、单文件内 ID 重复、读写失败都会
  指出问题文件和涉及的列，本次**不提供下载**。所有文件先预检，再开始写。
- **成功后打包**：按清单里的 `uploaded_filename`（原上传文件名）打包为
  `iceberg-field-ids.zip`。未出现在清单中的上传文件会列入警告。

## 内置样例

`sample/` 由 `tools/make_sample.py` 生成。数据副本写入时**完全不带字段 ID**，
`data-0001.parquet` 与 `data-0002.parquet` 都含两个 struct：

- `base_fare.fare_cents` → 父字段 ID `3` → 子映射 ID **31**
- `surcharge.fare_cents` → 父字段 ID `4` → 子映射 ID **41**

点 **“载入样例”** 会显示每个文件的 ID 清单；点 **“下载 ZIP”** 得到补好 ID
的两个副本。位置删除文件保持原样、不打包。

样例同时保留了原核对工具的数据：`data-0001.parquet` 票号 4242 在第 5 个行组
（全局行号 99），`data-0002.parquet` 无行组统计，票号 8888 在第 2 个行组。

## 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/` | 字段 ID 回填页面 |
| `POST` | `/backfill`（别名 `/field-ids`） | 上传并返回 ZIP |
| `GET` | `/sample/field-ids` | 运行内置样例并显示结果 |
| `GET` | `/sample/field-ids.zip` | 内置样例的 ZIP |
| `GET` | `/healthz` | 健康检查 |

`POST /backfill` 的表单字段：

| 字段 | 说明 |
| --- | --- |
| `manifest` | 清单文件，必填 |
| `files` | 一个或多个 Parquet 副本；也可混入 `name-mapping.json` |
| `name_mapping` | 直接粘贴的 JSON |
| `name_mapping_file` | 作为文件上传的 JSON |
| `mapping_input` | 可选：`text` / `file`，指定以哪个输入为准 |

成功时响应为 `application/zip`；失败时返回 400 和带文件/列清单的 HTML，绝不
附带任何 ZIP。

## 环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `MAX_UPLOAD_MB` | `512` | 单次请求上传上限 |
| `BACKFILL_BATCH_SIZE` | `65536` | 重写时每批行数 |
| `PORT` | `8000` | 本地 `python3 app.py` 监听端口 |

## 本地开发与测试

需要 Python 3.11+、Flask、PyArrow。

```bash
python3 -m pip install -r requirements.txt
python3 app.py                     # http://localhost:8000
python3 -m pytest -q               # 78 个测试
```

重新生成样例（会覆盖 `sample/` 下的 Parquet、清单和 name mapping）：

```bash
python3 tools/make_sample.py
```

## 目录

```
app.py                     Flask 路由、上传处理、ZIP 响应
checker/backfill.py        字段 ID 解析、分批重写、打包
checker/name_mapping.py    schema.name-mapping.default 解析
checker/manifest.py        清单解析与校验
checker/engine.py          原有票号位置删除核对
checker/errors.py          CheckError（文件 + 涉及的列）
templates/backfill.html    字段 ID 回填页面
templates/index.html       原有核对页面
static/style.css           样式
sample/                    内置样例（清单 + name mapping + Parquet）
tools/make_sample.py       样例生成脚本
tests/                     pytest 测试
Dockerfile                 容器镜像
docker-compose.yml         启动配置
```

## 原有核对工具

`/checker` 与 `GET /sample` 仍是原来的 Iceberg v2 位置删除核对：上传清单、
`data` / `position_delete` 副本，输入票号，按行组统计跳读并用原始行号匹配
位置删除。`POST /check` 在没有 name mapping 时走这条旧逻辑；一旦带上
`schema.name-mapping.default`，它就执行字段 ID 回填并返回 ZIP。

## 已知边界

- 位置删除文件会整体读入内存建立 `(file_path, pos)` 索引；适用于常见规模。
- 字段 ID 通过 PyArrow 的 `PARQUET:field_id` 字段元数据写入，PyArrow 会在
  Parquet 模式里写成 `field_id=`；不会改写 Iceberg 元数据 JSON。
- 重写会重新计算行组边界与统计信息；列值、列名、类型和行序不变。
- 每个文件最多只会被匹配一次；同名子列必须能通过父字段 ID 区分。缺少父 ID
  且映射存在歧义时会明确报错，而不是猜一个。
- 路径按字符串精确比较，不解析 `s3://`、`file://` 等 URI 等价关系。
