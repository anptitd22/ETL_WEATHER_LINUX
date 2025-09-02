# Giới thiệu 

Đề bài: ETL dữ liệu thời tiết 

Gọi api lấy dữ liệu dạng json trên trang https://openweathermap.org/

Đẩy dữ liệu lên minIO bằng boto3

Đọc, lọc lấy 1 vài field và tải dữ liệu bằng spark vào oracle, iceberg

Dùng trino kết nối truy vấn với iceberg thông qua nessie (lưu trữ dữ liệu trên postgre)

Xem cổng spark:

<img width="1793" height="760" alt="image" src="https://github.com/user-attachments/assets/195217ee-a82e-4eaf-8bf8-41db874d9968" />

Xem dags của airflow:

<img width="691" height="385" alt="image" src="https://github.com/user-attachments/assets/562c55cc-ac05-439f-bb2e-0b8f2302b4c5" />

Xem dữ liệu weather.json từ minIO:

<img width="1800" height="971" alt="image" src="https://github.com/user-attachments/assets/af6a8ad9-dd3a-4d68-97f6-958b972156fd" />

Xem dữ liệu bởi iceberg trên minIO:

<img width="1789" height="690" alt="image" src="https://github.com/user-attachments/assets/248dd2f0-868a-43b0-9a1c-3ed7ad6188d8" />

Truy cập trino trên docker và query:

<img width="830" height="1006" alt="image" src="https://github.com/user-attachments/assets/6e79d990-91ea-4328-9829-dc012cf9c1ba" />

Xem trino UI trên web:

<img width="1225" height="956" alt="image" src="https://github.com/user-attachments/assets/bdccdfe7-d400-4a86-89e8-1a4586067d93" />

Tổng quát:

<img width="768" height="137" alt="iceberg_nessie_flow_fixed" src="https://github.com/user-attachments/assets/a92ddf6e-8525-4b13-9683-0a759ead4e9a" />

# CÀI ĐẶT

Tải instantclient-basic-linux.x64-23.8.0.25.04.zip

https://drive.google.com/file/d/118ADdJCFTtUn9iUbtroeVdTdDpp_rE8f/view?usp=drive_link

Đặt vào install/instantclient-basic-linux.x64-23.8.0.25.04.zip

docker-compose build --no-cache

docker-compose up -d

#các thư viện cần trong python nằm trong requirements.txt

# Lưu ý

key api: tạo từ web

quy đổi thời gian bằng https://www.epochconverter.com

tài khoản airflow:

- tk: airflow
- mk: airflow

tài khoản minIO

- tk: admin
- tk: admin123456

tài khoản trino

-tk: $USER (tên user trên linux)

docker oracle (Tự cài) (nhớ là kết nối cùng network)

tài khoản tạo cho vào .env 
