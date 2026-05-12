# Büyük Veri Analizi Dönem Projesi

**Ders:** Büyük Veri Analizine Giriş — 2025-2026 Bahar Dönemi  
**Veri Seti:** World Energy Consumption (Kaggle) — ~17.000 kayıt, 122 ülke  
**Konu:** Ülke bazlı enerji tüketimi analizi ve tahmini

---

## Proje Mimarisi

```
CSV Dosyası
    │
    ▼
┌─────────────┐     JSON Mesajları      ┌─────────────┐
│   Producer  │ ──────────────────────► │    Kafka    │
│  (Python)   │   ~100 mesaj/saniye     │   Broker    │
└─────────────┘                         └──────┬──────┘
                                               │
                                               ▼
                                  ┌────────────────────────┐
                                  │   Spark Structured     │
                                  │      Streaming         │
                                  └────────────┬───────────┘
                                               │
                    ┌──────────────────────────┼──────────────────────────┐
                    ▼                          ▼                          ▼
             ┌────────────┐           ┌──────────────┐           ┌──────────────┐
             │   Bronze   │           │    Silver    │           │     Gold     │
             │ (Ham Veri) │──────────►│(Temizlenmiş) │──────────►│  (Agregat)   │
             └────────────┘           └──────────────┘           └──────────────┘
                    │                        │
                    ▼                        ▼
             ┌────────────┐       ┌──────────────────┐
             │    EDA     │       │    Feature       │
             │  Analizi   │       │  Engineering     │
             └────────────┘       └────────┬─────────┘
                                           │
                                           ▼
                                  ┌─────────────────┐
                                  │   ML Modelleri  │
                                  │  + MLflow       │
                                  └────────┬────────┘
                                           │
                                           ▼
                                  ┌─────────────────┐
                                  │   Dashboard &   │
                                  │ Görselleştirme  │
                                  └─────────────────┘
```

---

## Teknoloji Yığını

| Katman | Teknoloji | Versiyon |
|---|---|---|
| Konteynerizasyon | Docker, Docker Compose | latest |
| Mesaj Kuyruğu | Apache Kafka (KRaft) | latest |
| Veri İşleme | Apache Spark (PySpark) | 3.5.1 |
| Depolama | Delta Lake | 3.1.0 |
| Makine Öğrenmesi | Spark MLlib + MLflow | — |
| Notebook Ortamı | JupyterLab (PySpark) | spark-3.5.0 |
| Veri Üretici | Python | 3.11 |

---

## Gereksinimler

### Docker ile çalıştırma
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (v4.0+)
- En az **8 GB RAM** (Docker'a atanmış)
- En az **15 GB disk alanı**

### Yerel çalıştırma (Docker dışı)
- Python 3.12
- Java 17 (`brew install openjdk@17`)
- `JAVA_HOME` ortam değişkeni ayarlanmış olmalı

---

## Kurulum ve Çalıştırma

### Docker ile (Önerilen)

#### 1. Projeyi klonla

```bash
git clone https://github.com/<kullanici-adi>/BuyukVeri.git
cd BuyukVeri
```

#### 2. Docker image'larını build et

```bash
docker compose build
```

> İlk build ~5-10 dakika sürer (Jupyter image ~6 GB).

#### 3. Tüm servisleri başlat

```bash
docker compose up
```

#### 4. Servislere eriş

| Servis | Adres | Bilgi |
|---|---|---|
| JupyterLab | http://localhost:8888 | Token: `admin` |
| Spark Master UI | http://localhost:8080 | — |
| MLflow UI | http://localhost:5001 | — |
| Kafka | `localhost:9092` | — |

#### 5. Servisleri durdur

```bash
docker compose down
```

### Yerel Çalıştırma (Docker dışı)

Notebook'lar Docker olmadan da çalıştırılabilir. Java 17 kurulu ve `JAVA_HOME` ayarlı olması gerekir:

```bash
# Java kurulumu (macOS)
brew install openjdk@17

# ~/.zshrc veya ~/.bashrc dosyasına ekle
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"

# Bağımlılıkları yükle
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt   # veya venv zaten kuruluysa doğrudan çalıştır

# Jupyter başlat
jupyter lab
```

> MLflow UI yerel modda `http://localhost:5000` adresinde çalışır.

---

## Proje Yapısı

```
BuyukVeri/
├── docker-compose.yaml          # Tüm servislerin tanımı
├── README.md                    # Bu dosya
│
├── producer/
│   ├── Dockerfile               # Python 3.11-slim image
│   ├── requirements.txt         # pandas, kafka-python
│   └── producer.py              # Kafka'ya veri gönderen uygulama
│
├── jupyter/
│   └── Dockerfile               # Özel JupyterLab image
│
├── mlflow/
│   └── Dockerfile               # MLflow sunucu image
│
├── notebooks/
│   ├── 1_Docker_Ortaminin_Kurulumu.ipynb
│   ├── 3_Spark_Structured_Streaming_ile_Veri_Okuma.ipynb
│   ├── 3.5_Mimari.ipynb
│   ├── 4_Kesifsel_Veri_Analizi_EDA.ipynb
│   ├── 5_Feature_Engineering.ipynb
│   ├── 6_Makine_Ogrenmesi_MLflow.ipynb
│   └── 7_Dashboard_ve_Gorsellestirme.ipynb
│
└── data/
    ├── World Energy Consumption.csv   # Ham kaynak veri
    ├── bronze/                        # Delta Lake — ham akış verisi
    ├── silver/                        # Delta Lake — temizlenmiş veri
    ├── silver_features/               # Delta Lake — özellik tablosu
    ├── gold/                          # Delta Lake — agregat/özet veri
    ├── mlruns/                        # MLflow deney ve model kayıtları
    ├── figures/                       # Üretilen görsel çıktılar
    └── checkpoints/                   # Spark Streaming kontrol noktaları
```

---

## Notebooklar

### Notebook 1 — Docker Ortamının Kurulumu
Docker container'larının çalıştığını doğrular, Kafka topic listesini gösterir.

### Notebook 3 — Spark Structured Streaming ile Veri Okuma
Kafka'dan gelen JSON mesajlarını Spark Structured Streaming ile okur. Şema tanımlanır ve veri **Bronze** Delta Lake tablosuna yazılır. Ardından temizleme yapılıp **Silver** ve **Gold** katmanlarına aktarılır.

- Kafka connector: `spark-sql-kafka-0-10_2.12:3.5.1`
- Delta Lake: `delta-spark_2.12:3.1.0`

### Notebook 3.5 — Mimari
Projenin genel veri akışını ve katman mimarisini belgeler.

### Notebook 4 — Keşifsel Veri Analizi (EDA)
Bronze Delta tablosu `deltalake` Python kütüphanesiyle okunur.

- Temel istatistikler (17.432 kayıt, 126 sütun)
- Eksik değer analizi
- Zaman serisi analizi (saatlik trend)
- Kategorik ve sayısal değişken dağılımları

### Notebook 5 — Özellik Mühendisliği (Feature Engineering)
ML modeline beslenecek anlamlı özellikler üretilir ve **Silver** katmanına kaydedilir.

| Özellik | Açıklama | İş Mantığı |
|---|---|---|
| `is_weekend` | Hafta sonu mu? (0/1) | Tüketim alışkanlıkları hafta içi/sonu farklıdır |
| `time_of_day` | Günün vakti (Sabah/Öğle/Akşam/Gece) | Olay zamanı anormallik tespitinde kritik bağlamdır |
| `user_event_count` | Kullanıcının toplam işlem sayısı | Yüksek işlem hacmi Power User veya anomaliyi gösterir |
| `item_popularity` | İlgili öğenin toplam etkileşim sayısı | Sistem yükü dağılımı için önemlidir |
| `event_risk_score` | Olayın kritiklik puanı (1-5) | Arıza/uyarı olaylarına daha yüksek ağırlık verir |

### Notebook 6 — Makine Öğrenmesi ve MLflow
`primary_energy_consumption` hedef değişkeni üzerine **regresyon** problemi.

5 model eğitilir ve MLflow ile karşılaştırılır:
1. Linear Regression
2. Decision Tree Regressor
3. Random Forest Regressor
4. Gradient Boosted Trees (GBT) Regressor
5. Generalized Linear Regression (GLR)

Metrikler: RMSE, MAE, R², Feature Importance, Residual Analizi  
Sonuçlar ve model artifact'ları MLflow'a loglanır; en iyi modelin tahminleri **Gold** katmanına Delta formatında kaydedilir.

### Notebook 7 — Dashboard ve Görselleştirme
Tüm proje sonuçlarını özetleyen görsel dashboard.

- 5 modelin performans karşılaştırma grafiği
- Feature Importance grafiği
- Zaman serisi trend grafikleri
- Veri dağılım grafikleri
- Gerçek vs Tahmin scatter plot
- Residual dağılım grafiği

---

## Veri Seti

**World Energy Consumption** — [Kaggle Linki](https://www.kaggle.com/datasets/pralabhpoudel/world-energy-consumption)

- **Kayıt sayısı:** ~17.432
- **Sütun sayısı:** 122
- **Kapsam:** 122 ülke, 1965-2022 yılları
- **Hedef değişken:** `primary_energy_consumption` (TWh)
- **Temel alanlar:** Kömür, gaz, yağ, yenilenebilir enerji tüketimi; üretim verileri; kişi başı tüketim; GDP

---

## Pipeline Akışı (Adım Adım)

```
1. docker compose up → tüm servisler ayağa kalkar
2. Kafka sağlıklı hale geldiğinde Producer otomatik başlar
3. Producer, CSV'yi okuyup saniyede ~100 JSON mesajı Kafka'ya gönderir
4. Notebook 3 çalıştırılır → Spark, Kafka'dan okur → Bronze Delta'ya yazar
5. Notebook 4 çalıştırılır → EDA grafikleri üretilir
6. Notebook 5 çalıştırılır → Özellikler üretilip Silver Delta'ya kaydedilir
7. Notebook 6 çalıştırılır → 5 model eğitilir, MLflow'a loglanır
8. Notebook 7 çalıştırılır → Dashboard grafikleri oluşturulur
```

---

## Değerlendirme Kriterleri

| Kriter | Ağırlık |
|---|---|
| Docker & Altyapı | %15 |
| Kafka Streaming | %15 |
| Spark + Delta Lake | %15 |
| EDA & Feature Engineering | %10 |
| ML Modelleri & MLflow | %15 |
| Dashboard & Görselleştirme | %15 |
| Dokümantasyon & Sunum | %15 |
