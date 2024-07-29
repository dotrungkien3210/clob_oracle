# Nifi Query Clob Oracle

## Introduction

Project này hướng đến việc xử lý oracle, lấy ra các trường, trong đó có trường Clob của oracle

### Problem
Đối với clob, vấn đề đặt ra là khi fetch về resultset thì sẽ có dạng byte, như vậy khi convert object về sẽ bị mất trường đó 
### Idea

Khi mà load về ta sẽ xử lý load kiểm tra clob, nếu như xuất hiện trường đó thì xử lý để đưa về dạng string

### Benefit
Hàm này được custom riêng cho clob, giảm đáng kể code check được implement, giúp tăng tốc độ xử lý

## Implementation

### Step

Đầu tiên clone code về


Tiếp theo thực hiện kiểm tra


Kế tiếp build thành file nar

Thực hiện copy file nar vào thư mục lib của nifi, sau đó khởi động lại chương trình

Tìm tới Processor ExtractOracleRecord, setup các Properties và Controller Service

Chạy thử và nhận kết quả


### Problem
Không có problem đáng kể nào hiện tại.