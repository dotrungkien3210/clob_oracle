# Nifi Query Clob Oracle

## Introduction

Project này hướng đến việc xử lý oracle, lấy ra các trường, trong đó có trường Clob của oracle

### Problem
- Thử sử dụng processor ExecuteSQLRecord

Query thử bản ghi oracle thấy kết quả bình thường nhưng với các paragraph lớn hiện được report là kết quả đôi khi không chuẩn xác


- Thử xây dựng một processor dành riêng cho oracle

Đối với clob, vấn đề đặt ra là khi fetch về resultset thì sẽ có dạng byte, như vậy khi convert object để trả về sẽ bị mất trường đó. 

### Idea

Khi mà load về ta sẽ xử lý load kiểm tra trong resultset liệu có chứa trường clob, nếu như xuất hiện trường đó thì xử lý để đưa về dạng String

### Benefit
Hàm này được custom riêng cho clob, giảm đáng kể code check được implement. 
Vì vậy có thể kì vọng performance tốt.

## Implementation

### Step Implementation

- Đầu tiên clone code về
```
git clone git@github.com:dotrungkien3210/clob_oracle.git
```
- Tiếp theo thực hiện kiểm tra
```
mvn validate
```

- Kế tiếp build thành file nar
```dtd
mvn clean install
```
Nếu thành công, một file nar sẽ được lưu trong folder nar
![img_1.png](img_1.png)
Thực hiện copy file nar vào thư mục lib của nifi, sau đó khởi động lại chương trình nifi
![img.png](img.png)
Tìm tới Processor ExtractOracleRecord, setup các Properties và Controller Service

Properties
![img_2.png](img_2.png)
Controller Service (Chú ý rằng mới chỉ thực hiện xử lý câu lệnh select)
![img_3.png](img_3.png)
Chạy thử và nhận kết quả
![img_4.png](img_4.png)


### Problem
Hiện chưa thể đưa về một output có kiểu dữ liệu hoàn chỉnh, ý là chưa thể chọn output là CSV, JSON... 
Output hiện tại vẫn có dạng String và phải thêm một processor nữa để xử lý

PartitionRecord
![img_5.png](img_5.png)
Config gợi ý
![img_6.png](img_6.png)
