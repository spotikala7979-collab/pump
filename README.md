# Fero Radar — Streamlit Edition

Binance Futures pump/radar ve whale dashboard.  
Streamlit Community Cloud'da **ücretsiz** çalışır.

---

## 🚀 Yayına Alma (5 dakika)

### 1. GitHub'a yükle

Bu 2 dosyayı GitHub'da yeni bir repo'ya yükle:
- `streamlit_app.py`
- `requirements.txt`

Repo adı örnek: `fero-radar`

### 2. Streamlit Cloud'a bağla

1. [share.streamlit.io](https://share.streamlit.io) adresine git
2. GitHub hesabınla giriş yap
3. **"New app"** → az önce oluşturduğun repo'yu seç
4. Main file: `streamlit_app.py`
5. **Deploy** 🎉

### 3. Hazır

Streamlit sana bir URL verir (örnek: `https://fero-radar-xyz.streamlit.app`).  
Bu URL'yi tarayıcıda aç, uygulama çalışıyor.

---

## 🖥️ Yerel test

```bash
pip install streamlit httpx websockets pandas
streamlit run streamlit_app.py
```

---

## 📊 Özellikler

| Özellik | Durum |
|---|---|
| BTC 5dk / 15dk değişim | ✅ |
| Radar FLASH sinyalleri | ✅ |
| Radar CONFIRMED sinyalleri | ✅ |
| Whale SWEEP takibi | ✅ |
| Whale LIQ (tasfiye) takibi | ✅ |
| Otomatik yenileme | ✅ (2-15 sn ayarlanabilir) |
| Redis gereksiz | ✅ |
| Docker gereksiz | ✅ |

---

## ⚠️ Notlar

- **İlk CONFIRMED sinyal** ~15 dakika sonra gelir (radar 15 dakikalık geçmiş veri ister).
- **FLASH sinyaller** çok daha erken gelir.
- Streamlit Cloud uygulamayı uzun süre ziyaret edilmezse uyutur.  
  Yeniden açınca 30 saniye içinde uyanır.
- Finansal tavsiye değildir.
