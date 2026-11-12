import time
import happybase

print("=" * 70)
print(">>> [SERVING LAYER] KẾT NỐI TỚI HBASE NOSQL QUA THRIFT PORT 9090...")
print("=" * 70)

try:
    # 1. Ket noi den HBase
    connection = happybase.Connection('localhost', port=9090)
    connection.open()
    table = connection.table('order_tracking')
    print(">>> Kết nối thành công tới bảng HBase 'order_tracking'!")

    # 2. Thu nghiem tra cuu nhanh theo RowKey
    test_order_id = 'order_3158819'
    print(f"\n>>> Đang thực hiện Point-Lookup cho mã: {test_order_id}...")

    t0 = time.time()
    row = table.row(test_order_id.encode('utf-8'))
    latency_ms = round((time.time() - t0) * 1000, 2)

    print("=" * 70)
    print(f"KẾT QUẢ TRA CỨU ĐƠN HÀNG (ĐỘ TRỄ PHẢN HỒI: {latency_ms} ms):")
    print("=" * 70)
    for key, value in row.items():
        print(f"  + {key.decode('utf-8')}: {value.decode('utf-8')}")

    connection.close()

except Exception as e:
    print(f">>> Lỗi kết nối HBase: {e}")
