import streamlit as st
import happybase
import time
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Alibaba Cainiao Big Data Lakehouse Dashboard", layout="wide", page_icon="📦")

st.title("📦 HỆ THỐNG QUẢN TRỊ CHUỖI CUNG ỨNG & DỰ BÁO GIAO HÀNG (ALIBABA LA-DE)")
st.caption("Kiến trúc Big Data Lakehouse: Hadoop HDFS + Apache Spark + NoSQL HBase")

# Kết nối HBase qua Thrift
@st.cache_resource
def get_hbase_conn():
    try:
        conn = happybase.Connection('localhost', port=9090)
        conn.open()
        return conn
    except Exception as e:
        return None

conn = get_hbase_conn()

# 4 Tab chức năng
tab1, tab2, tab3, tab4 = st.tabs([
    "🔍 Tra cứu Đơn hàng (Point-Lookup)", 
    "📑 Danh sách Dữ liệu NoSQL (Table Scan)", 
    "📈 Phân tích Khung giờ & Rủi ro Trễ (OLAP)", 
    "💾 Thêm Đơn hàng mới"
])

# ----------------- TAB 1: POINT-LOOKUP -----------------
with tab1:
    st.subheader("Tra cứu Tức thì Đơn hàng từ HBase NoSQL (Độ trễ < 10ms)")
    col1, col2 = st.columns([3, 1])
    with col1:
        order_input = st.text_input("Nhập mã đơn hàng (RowKey):", value="order_1015570")
    with col2:
        search_btn = st.button("🚀 Tra cứu ngay", use_container_width=True)

    if search_btn or order_input:
        if conn is None:
            st.error("Không thể kết nối tới HBase Thrift Server (Port 9090)!")
        else:
            try:
                table = conn.table('order_tracking')
                t0 = time.time()
                data = table.row(order_input.encode('utf-8'))
                latency = round((time.time() - t0) * 1000, 2)

                if data:
                    res = {k.decode('utf-8').replace('cf:', ''): v.decode('utf-8') for k, v in data.items()}
                    st.success(f"⚡ Tìm thấy dữ liệu! Độ trễ phản hồi từ HBase: **{latency} ms**")
                    
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Thành phố", res.get("city", "N/A"))
                    c2.metric("Shipper ID", res.get("courier_id", "N/A"))
                    c3.metric("Thời gian giao thực tế", f"{res.get('duration_min', 'N/A')} phút")
                    
                    risk = res.get("late_risk", "0")
                    if risk == "1":
                        c4.error("🚨 NGUY CƠ GIAO TRỄ (LATE)")
                    else:
                        c4.success("✅ GIAO ĐÚNG HẠN (ON-TIME)")
                else:
                    st.warning(f"Không tìm thấy đơn hàng '{order_input}' trong cơ sở dữ liệu HBase!")
            except Exception as e:
                st.error(f"Lỗi truy vấn: {e}")

# ----------------- TAB 2: FULL DATA LIST (SCAN TABLE) -----------------
with tab2:
    st.subheader("Liệt kê Danh sách Dữ liệu NoSQL HBase (HBase Table Scanner)")
    c_scan1, c_scan2 = st.columns([2, 1])
    with c_scan1:
        limit_rows = st.slider("Số lượng bản ghi muốn tải từ HBase:", min_value=50, max_value=3000, value=1000, step=50)
    with c_scan2:
        reload_btn = st.button("🔄 Tải lại dữ liệu", use_container_width=True)

    if conn:
        try:
            tbl = conn.table('order_tracking')
            t0 = time.time()
            rows_generator = tbl.scan(limit=limit_rows)
            data_list = []
            for k, v in rows_generator:
                item = {"Mã Đơn hàng (RowKey)": k.decode('utf-8')}
                for col_name, col_val in v.items():
                    clean_name = col_name.decode('utf-8').replace("cf:", "")
                    item[clean_name] = col_val.decode('utf-8')
                data_list.append(item)
            scan_time = round((time.time() - t0) * 1000, 2)

            if data_list:
                df_scan = pd.DataFrame(data_list)
                st.info(f"Đã quét thành công **{len(df_scan)}** bản ghi từ HBase trong **{scan_time} ms**.")
                
                # Bộ lọc thành phố
                all_cities = ["Tất cả", "Shanghai", "Hangzhou", "Chongqing", "Yantai", "Jilin"]
                selected_city = st.selectbox("Lọc theo thành phố:", all_cities)
                if selected_city != "Tất cả" and "city" in df_scan.columns:
                    df_scan = df_scan[df_scan["city"] == selected_city]

                # Định dạng tên cột trực quan
                display_df = df_scan.rename(columns={
                    "city": "Thành phố",
                    "courier_id": "Mã Shipper",
                    "duration_min": "Thời gian giao (phút)",
                    "late_risk": "Rủi ro trễ hạn (1=Trễ, 0=Đúng)"
                })

                st.dataframe(display_df, use_container_width=True, height=450)
            else:
                st.warning("Hiện tại chưa có dữ liệu trong bảng.")
        except Exception as e:
            st.error(f"Lỗi scan HBase: {e}")

# ----------------- TAB 3: OLAP & ML ANALYTICS -----------------
with tab3:
    st.subheader("Phân tích Nhu cầu & Rủi ro Giao trễ theo Khung giờ (Spark OLAP)")
    
    # Bảng số liệu phân tích khung giờ từ Giai đoạn 3
    sample_hourly_data = {
        "Khung giờ": [f"{h}:00" for h in range(7, 21)],
        "Tổng đơn hàng": [12000, 35000, 68000, 95000, 84000, 62000, 71000, 89000, 110000, 98000, 75000, 51000, 32000, 15000],
        "Tỷ lệ trễ (%)": [12.5, 14.2, 18.9, 24.5, 22.1, 16.3, 17.8, 26.4, 28.9, 25.1, 19.3, 15.4, 13.1, 11.8]
    }
    df_chart = pd.DataFrame(sample_hourly_data)

    c_chart1, c_chart2 = st.columns(2)
    with c_chart1:
        fig1 = px.bar(df_chart, x="Khung giờ", y="Tổng đơn hàng", title="Lưu lượng Đơn hàng theo Khung giờ trong Ngày", color_discrete_sequence=["#00ADB5"])
        st.plotly_chart(fig1, use_container_width=True)
    with c_chart2:
        fig2 = px.line(df_chart, x="Khung giờ", y="Tỷ lệ trễ (%)", title="Biến động Tỷ lệ Giao trễ (Đỉnh điểm lúc 11h & 15h)", markers=True, color_discrete_sequence=["#FF5722"])
        st.plotly_chart(fig2, use_container_width=True)

# ----------------- TAB 4: INSERT DATA -----------------
with tab4:
    st.subheader("Thêm Đơn hàng mới trực tiếp vào bảng 'order_tracking'")
    with st.form("insert_form"):
        new_id = st.text_input("Mã đơn hàng mới (Ví dụ: order_8888888)")
        new_city = st.selectbox("Thành phố", ["Shanghai", "Hangzhou", "Chongqing", "Yantai", "Jilin"])
        new_duration = st.number_input("Thời gian giao (phút)", min_value=1, value=150)
        new_risk = st.selectbox("Trạng thái dự báo", ["0 - Giao đúng hạn", "1 - Trễ hạn"])
        
        submit_btn = st.form_submit_button("💾 Lưu vào HBase")
        if submit_btn:
            if conn and new_id:
                tbl = conn.table('order_tracking')
                risk_val = "1" if "1" in new_risk else "0"
                tbl.put(new_id.encode('utf-8'), {
                    b'cf:city': new_city.encode('utf-8'),
                    b'cf:duration_min': str(new_duration).encode('utf-8'),
                    b'cf:late_risk': risk_val.encode('utf-8')
                })
                st.success(f"🎉 Đã ghi thành công đơn hàng **{new_id}** vào HBase NoSQL!")
            else:
                st.error("Vui lòng nhập đầy đủ mã đơn hàng và đảm bảo HBase đang mở!")
