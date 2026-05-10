import os
import pandas as pd
import kafka
from kafka import KafkaProducer
import time
import json
from datetime import datetime, timezone




producer = kafka.KafkaProducer(
    bootstrap_servers=['kafka:29092'],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def stream_data():
   
   path = "/app/data/World Energy Consumption.csv"

   if not os.path.exists(path):
       print(f"CSV dosyası bulunamadı: {path}")
       return
   
   total_sent = 0
   
   df= pd.read_csv(path)
   df = df.sample(frac=1).reset_index(drop=True) # Verileri karıştırarak gönderme kaldırılabilir

   
   print(f"[{time.strftime('%H:%M:%S')}] Akış Başladı", flush=True)

   for index, row in df.iterrows():
      
      record = row.to_dict()
      message= {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "user_id": "admin",
            "event_type": "Enerji Tüketimi",
            "related_id": f"{record.get('iso_code', 'unknown')}-{record.get('year', 'unknown')}",
            "data": record        
      }

      producer.send("world_energy_consumption", value=message)
      total_sent += 1

      country = record.get('country', 'N/A')
      year = record.get('year', 'N/A')
      energy = record.get('primary_energy_consumption', 0)

      #print(f"[{time.strftime('%H:%M:%S')}] Gönderildi -> timestamp: {message['timestamp']} | user_id: {message['user_id']} |event_type: {message['event_type']} | related_id: {message['related_id']} ", flush=True)

      #print(f"[{time.strftime('%H:%M:%S')}] Gönderildi -> Ülke: {country:<15} | Yıl: {year} | Tüketim: {energy}", flush=True)

      if total_sent % 50 == 0:
        #print(f"[{time.strftime('%H:%M:%S')}] Gönderildi -> timestamp: {message['timestamp']} | user_id: {message['user_id']} |event_type: {message['event_type']} | related_id: {message['related_id']} ", flush=True)

        print(f"[{time.strftime('%H:%M:%S')}] Gönderildi -> Ülke: {country} | Yıl: {year} | Tüketim: {energy}", flush=True)


      if total_sent % 100 == 0:
         print(f"[{time.strftime('%H:%M:%S')}] Toplam {total_sent} veri başarıyla gönderildi", flush=True)


      time.sleep(0.01)  # Mesaj gönderme hızı saniyede 100 mesaj 

   print(f"[{time.strftime('%H:%M:%S')}] Akış Tamamlandı. Toplam {total_sent} veri gönderildi.", flush=True)
   
   '''while True:
    time.sleep(1000)
   '''
    
    
 
if __name__ == "__main__":
   
   time.sleep(10)
   stream_data()
   