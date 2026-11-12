# 渡轮结算 · Iceberg Parquet 字段 ID 回填

恢复渡轮结算历史分区时，旧的 Parquet 副本里没有 Iceberg 字段 ID，查询引擎
无法按 ID 读到票价等列。本工具读取表属性 **`schema.name-mapping.default`**
的 JSON 内容，把缺失的字段 ID 写回 Parquet 模式（`PARQUET:field_id`），
列名、类型、值与行序保持不变。

只处理**标量列和一层 struct**；list / map 与嵌套 struct 会被拒绝。

## 快速启动

```bash
docker compose up --build
```

打开 <http://localhost:8000>，点 **“载入样例”** 再提交即可下载回填后的 ZIP。
接口健康检查：

```bash
curl http://localhost:8000/healthz
```

停止：

```bash
docker compose down
```

## 网页用法

1. 上传 **有效文件清单**（CSV 或 JSON，沿用现有清单格式）。
2. 上传清单中所有 `data` / `position_delete` 文件的 **Parquet 副本**，可多选。
   文件名必须与清单的 `uploaded_filename` 一致。
3. 粘贴表属性 `schema.name-mapping.default` 的 **JSON 内容**。
4. 提交。整批成功后浏览器下载 `field-ids-backfill.zip`，ZIP 内每个文件保持
   原上传文件名；原清单和位置删除文件继续使用，无需改动。

上传文件只保存在一次请求的临时目录中，请求结束后自动删除；不会按用户文件名
拼接服务器路径。

### 清单格式

与既有 Iceberg v2 有效文件清单完全相同：

```csv
original_path,uploaded_filename,file_type,data_sequence_number
s3://ferry-lake/warehouse/settlement/voyage_date=2026-11-05/data/data-0001.parquet,data-0001.parquet,data,7
s3://ferry-lake/warehouse/settlement/voyage_date=2026-11-05/delete-pos/delete-pos-0001.parquet,delete-pos-0001.parquet,position_delete,12
```

`file_type` 为 `equality_delete` 的条目按规则忽略：不要求上传副本，也不会
出现在结果 ZIP 中。

### name-mapping 格式

`schema.name-mapping.default` 是一个映射数组（也接受单个映射对象，或用
`{"fields": [...]}` 包裹的对象）：

```json
[
  {"field-id": 1, "names": ["ticket_no"]},
  {
    "field-id": 20,
    "names": ["deck_fare", "deck_settlement"],
    "fields": [
      {"field-id": 21, "names": ["amount", "fare_amount", "price"]},
      {"field-id": 22, "names": ["currency", "ccy"]}
    ]
  }
]
```

- `names` 是该字段 ID 接受的全部名称/别名。
- `fields` 给出 struct 的子映射，作用域隔离，不同 struct 下的同名列互不干扰。

## 回填规则

- **已有 ID 不动**：Parquet 字段上已存在合法的非负 `PARQUET:field_id` 时
  原样保留；元数据缺失、无法解析或为 Parquet 哨兵值 `-1` 时视为缺 ID。
- **缺 ID 按别名唯一匹配**：在当前作用域内用 Parquet 列名匹配映射节点的
  `names`。匹配不到、或同时匹配到多个字段 ID 都会报错，不做猜测。
- **子列先用父字段 ID 定位子映射**：struct 自身解析出字段 ID 后，其所有
  子列只在该 ID 对应映射节点的 `fields` 内匹配。因此两个 struct 下同名的
  `amount` 会各自拿到各自的 ID，不会按叶列名混淆。
- **单文件内 ID 唯一**：任何两个字段（含 struct 与其子列）解析出同一个
  字段 ID 都会中止该批处理。
- **只支持标量与一层 struct**：遇到 list、map 或嵌套 struct 时报错并给出
  文件与列路径（如 `deck_fare.amount`）。
- **分批重写、内容保真**：逐行组读取并写回，保持行组边界与行数；列名、
  Arrow 类型、可空性、数据值、行序不变；每列压缩方式、列统计开关与
  schema 级 key/value 元数据保留。
- **全有或全无**：先对全部副本完成纯校验规划，任何一个文件存在缺副本、
  无法唯一匹配、ID 重复或读写失败，整批中止——错误页面列出问题文件和涉及
  列，不提供任何下载；校验全部通过后才开始重写。

成功响应附带三个汇总响应头，便于不拆包即可核对：

| 响应头 | 含义 |
| --- | --- |
| `X-Files-Rewritten` | 重写的副本数 |
| `X-Field-Ids-Assigned` | 本次新写入的字段 ID 数量 |
| `X-Field-Ids-Kept` | 原本已存在、原样保留的字段 ID 数量 |

## 内置样例

`sample/` 中的演示数据由 `tools/make_sample.py` 生成：

- `data-0001.parquet` / `data-0002.parquet`：旧副本只有 `ticket_no` 带字段
  ID 1，其余标量列和两个 struct（`deck_fare`、`cabin_fare`）的子列都缺 ID。
- 两个 struct 下各有一个同名金额列 `amount`：提交后分别补上 **21** 与
  **31**，演示按父字段 ID 进入各自子映射的作用域匹配。
- `fare_note` 在映射中通过别名 `remark` 唯一匹配到字段 ID **5**。
- `data-0002.parquet` 使用混合压缩（gzip / zstd / snappy / 未压缩）并关闭
  列统计，用于验证这些属性在重写后保持不变。
- `delete-pos-0001.parquet` / `delete-pos-0002.parquet`：位置删除文件同样
  被回填（`file_path`=41、`pos`=42）；数据副本的列名、值与行序未变，原
  清单和删除文件继续可用。
- `name-mapping.json`：样例使用的 `schema.name-mapping.default` 内容。

## 本地开发与测试

需要 Python 3.11+、Flask、PyArrow。

```bash
python3 -m pip install -r requirements.txt
python3 app.py                     # http://localhost:8000
python3 -m pytest -q               # 67 个测试
```

重新生成样例（会覆盖 `sample/` 下的 Parquet、清单与 name-mapping）：

```bash
python3 tools/make_sample.py
```

## 目录

```
app.py                     Flask 路由、上传处理、ZIP 打包与临时目录清理
checker/manifest.py        清单解析与校验（CSV / JSON，列名别名）
checker/namemapping.py     schema.name-mapping.default 解析与作用域查询
checker/engine.py          字段 ID 规划、唯一性校验、逐行组重写
checker/errors.py          CheckError（始终带文件名与列名）
templates/index.html       网页
static/style.css           样式
sample/                    内置样例（清单 + name-mapping + Parquet）
tools/make_sample.py       样例生成脚本
tests/                     pytest 测试
Dockerfile                 容器镜像
docker-compose.yml         启动配置
```

## 已知边界

- 只支持标量列与一层 struct；list、map 与超过一层的嵌套 struct 直接拒绝。
- 整个文件按行组读入内存重写，适用于常见规模的历史分区副本。
- 名称匹配区分大小写，并按映射中的 `names` 精确比较，不做大小写折叠。
