# Risk Level Prediction Pipeline

本專案為風險預測與條件式分群工具，整合資料撈取、變數處理、模型預測與風險分類流程，透過指定日期參數，可彈性執行任一時間點的分析。

---

## Python版本:(套件參考requirements.txt)
Python 3.10.6 

## 使用方法

在終端機中執行以下指令以啟動整體流程：

```bash
python main.py --time 2023-12-31 --steps 111 --test_mode
```

### 參數說明：

- `--time`（必填）：指定要分析的資料日期，格式為 `YYYY-MM-DD`，此日期將用於資料撈取與預測結果的存檔資料夾名稱。目前只能選擇2023-06-30
- `--steps`（必填）：長度為 3 的數字字串（預設為 `"111"`），控制三個步驟是否執行：
  - 第 1 位：`1` 表示執行 `get_data`（資料處理）
  - 第 2 位：`1` 表示執行 `model_predict`（模型預測）
  - 第 3 位：`1` 表示執行 `clustering`（風險分群）
- `--test_mode`（必填）：外部測試做使用(使用範例資料)

例如：
- `--steps 100`：只執行資料處理
- `--steps 011`：僅執行模型預測與分群

##  輸出結果:
- 所有中介與最終結果將輸出至 `predict_result/{date}/`
  - `data.csv`                          指定日期下的個人特徵
  - `log.txt`                           執行紀錄
  - `unknown_labels.csv `               未知類別紀錄
  - `prediction_suicide_90_md5.csv`     自殺未來90天的各模型預測機率值
  - `prediction_suicide_180_md5.csv`    自殺未來180天的各模型預測機率值
  - `prediction_hurt_90_md5.csv`        傷人未來90天的各模型預測機率值
  - `prediction_hurt_180_md5.csv`       傷人未來180天的各模型預測機率值
  - `risk_level_suicide_90.csv`         自殺未來90天的風險等級結果  
  - `risk_level_suicide_180.csv`        自殺未來180天的分群與風險等級結果
  - `risk_level_hurt_90.csv`            傷人未來90天的分群與風險等級結果
  - `risk_level_hurt_180.csv`           傷人未來180天的分群與風險等級結果


---

## 專案結構說明

```
project_risk/
│
├── libs/                         # 自定函式模組
│   ├── connect_sql_function.py         # 資料庫連線工具
│   ├── variable_function.py            # 變數前處理邏輯
│   └── get_data_function.py            # 撈取並組裝分析資料的主函式
│
├── pipeline/                     # 預測流程與設定檔
│   ├── models/                          # 儲存訓練好的模型與對應的 LabelEncoder
│   ├── get_data.py                      # 依指定日期撈取並處理資料
│   ├── model_predict.py                 # 執行模型預測
│   ├── clustering.py                    # 根據模型預測結果進行條件式分群
│   ├── config.json                      # 變數表與資料來源設定
│   ├── model_rule.json                  # 各模型使用欄位與風險分群規則設定
│   ├── mapping_encoding.json            # 各模型label_encoding 類別mapping
│   └── sample_data.csv                  # 外部測試資料範例
│
├── predict_result/              # 輸出資料夾
│   └── {yyyy-mm-dd}/                    # 以分析日期為子目錄
│       ├── data.csv                     # 處理後資料
│       ├── prediction.csv               # 模型預測結果
│       ├── risk_level.csv               # 分群與風險等級結果
│       ├── unknown_labels.csv           # 未知類別紀錄
│       └── log.txt                      # 執行紀錄
│
├── main.py                      # 主控腳本，整合整體流程
├── requirements.txt             # Python 套件需求清單
└── README.md                    # 本說明文件
```

---

## 各模組說明

### `pipeline/get_data.py`

- **功能**：撈取指定日期的資料，進行變數處理與儲存。
- **Input**：
  - `pipeline/config.json`：變數設定
  - CLI 參數 `--date`：欲分析的日期（格式：YYYY-MM-DD）
- **Output**：
  - `predict_result/{date}/data.csv`

---

### `pipeline/model_predict.py`

- **功能**：讀取處理後資料，使用指定模型執行預測。
- **Input**：
  - `predict_result/{date}/data.csv`
  - `pipeline/config.json`
  - `pipeline/model_rule.json`
  - `pipeline/models/` 資料夾中的模型與 LabelEncoder
- **Output**：
  - `predict_result/{date}/prediction.csv`

---

### `pipeline/clustering.py`

- **功能**：根據模型預測結果與規則設定進行分群與風險分類。
- **Input**：
  - `predict_result/{date}/prediction.csv`
  - `pipeline/model_rule.json`
- **Output**：
  - `predict_result/{date}/risk_level.csv`

---

### `main.py`

- **功能**：整合整個分析流程，依輸入日期自動執行資料處理、預測與分群。
- **執行順序**：
  1. 撈取並處理資料
  2. 執行模型預測
  3. 進行風險分類與分群
  4. 輸出所有結果至指定資料夾
- **Input**：
  - `--time`：分析日期（YYYY-MM-DD）
  - `--steps`（必填）：長度為 3 的數字字串（預設為 `"111"`），控制三個步驟是否執行：
    - 第 1 位：`1` 表示執行 `get_data`（資料處理）
    - 第 2 位：`1` 表示執行 `model_predict`（模型預測）
    - 第 3 位：`1` 表示執行 `clustering`（風險分群）
  - `--test_mode`（必填）：外部測試做使用(使用範例資料)
- **Output**：
  - 所有中介與最終結果將輸出至 `predict_result/{date}/`
