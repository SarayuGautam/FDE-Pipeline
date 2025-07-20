CREATE SCHEMA IF NOT EXISTS transport;


CREATE TABLE transport.bus_routes (
route_code VARCHAR(20) PRIMARY KEY,
route_name VARCHAR(200),
pickup_location VARCHAR(100),
drop_location VARCHAR(100),
monthly_fee DECIMAL(8,2)
);


CREATE TABLE transport.student_transport_requests (
request_id VARCHAR(20) PRIMARY KEY,
student_id INTEGER,
route_code VARCHAR(20),
request_date DATE,
monthly_fee DECIMAL(8,2),
request_status VARCHAR(20)
);

INSERT INTO transport.bus_routes VALUES
('RT001', 'Ringroad Express', 'Main Campus', 'Chabahil', 120.00),
('RT002', 'Lalitpur Connect', 'Main Campus', 'Gwarko', 100.00),
('RT003', 'Bhaktapur Shuttle', 'Main Campus', 'Suryabinayak', 180.00);

INSERT INTO transport.student_transport_requests VALUES
('REQ001', 4001, 'RT001', '2024-08-15', NULL, NULL),
('REQ002', 4002, 'RT002', '2024-08-16', 100.00, 'APPROVED'),
('REQ003', 4003, 'RT003', '2024-08-17', NULL, NULL);

UPDATE transport.student_transport_requests sr
SET monthly_fee = br.monthly_fee
FROM transport.bus_routes br
WHERE sr.monthly_fee IS NULL
AND sr.route_code = br.route_code;

UPDATE transport.student_transport_requests
SET request_status = 
CASE
WHEN monthly_fee IS NOT NULL AND monthly_fee <= 150.00
THEN 'APPROVED'
WHEN monthly_fee IS NOT NULL AND monthly_fee > 150.00 THEN 'PENDING_REVIEW'
ELSE 'INCOMPLETE'
END
WHERE request_status IS NULL;
