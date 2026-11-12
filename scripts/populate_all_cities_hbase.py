import csv
import happybase
from datetime import datetime

def main():
    print("Ket noi toi HBase Thrift Server...")
    conn = happybase.Connection('localhost', 9090)
    conn.open()
    tbl = conn.table('order_tracking')

    cities_files = [
        ('Shanghai', '/home/dongnv/datasets/lade/delivery/delivery_sh.csv'),
        ('Hangzhou', '/home/dongnv/datasets/lade/delivery/delivery_hz.csv'),
        ('Chongqing', '/home/dongnv/datasets/lade/delivery/delivery_cq.csv'),
        ('Yantai', '/home/dongnv/datasets/lade/delivery/delivery_yt.csv'),
        ('Jilin', '/home/dongnv/datasets/lade/delivery/delivery_jl.csv')
    ]

    total_loaded = 0
    with tbl.batch(batch_size=500) as b:
        for city_name, fpath in cities_files:
            count = 0
            with open(fpath, mode='r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    oid = row.get("order_id")
                    if not oid:
                        continue
                    order_id = f"order_{oid}"
                    courier_id = row.get("courier_id", "N/A")
                    acc_t = row.get("accept_time")
                    del_t = row.get("delivery_time")
                    duration = 120.0
                    late_risk = "0"
                    if acc_t and del_t:
                        try:
                            t1 = datetime.strptime(f"2026-{acc_t}", "%Y-%m-%d %H:%M:%S")
                            t2 = datetime.strptime(f"2026-{del_t}", "%Y-%m-%d %H:%M:%S")
                            duration = round((t2 - t1).total_seconds() / 60.0, 1)
                            if duration < 0:
                                duration = 120.0
                        except:
                            pass
                    if duration > 180.0:
                        late_risk = "1"
                    
                    b.put(order_id.encode('utf-8'), {
                        b'cf:city': city_name.encode('utf-8'),
                        b'cf:courier_id': courier_id.encode('utf-8'),
                        b'cf:duration_min': str(duration).encode('utf-8'),
                        b'cf:late_risk': late_risk.encode('utf-8')
                    })
                    count += 1
                    if count >= 600:
                        break
            total_loaded += count
            print(f"Da nap {count} don hang cho thanh pho: {city_name}")

    print(f"==> Hoan tat! Tong cong da nap: {total_loaded} don hang tu 5 thanh pho vao HBase!")

if __name__ == "__main__":
    main()
