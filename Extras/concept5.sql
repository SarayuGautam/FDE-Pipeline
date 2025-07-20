CREATE SCHEMA IF NOT EXISTS community;

CREATE TABLE community.membership_target (
membership_id VARCHAR(20),
student_id INTEGER,
community_name VARCHAR(100),
membership_type VARCHAR(50),
join_date DATE,
annual_fee DECIMAL(8,2),
membership_status VARCHAR(20) DEFAULT 'ACTIVE',
PRIMARY KEY (membership_id, student_id)
);

TRUNCATE TABLE community.membership_temporary;


INSERT INTO community.membership_temporary VALUES
('MEM001', 5001, 'Creators', 'REGULAR', '2024-09-01', 50.00, DEFAULT),
('MEM002', 5002, 'UI Visuals', 'LEADER', '2024-09-02', 75.00, DEFAULT),
('MEM001', 5003, 'Ethical HCK', 'STEERING LEADER', '2024-09-01', 60.00, DEFAULT);


INSERT INTO community.membership_target
SELECT DISTINCT ON (membership_id, student_id)
membership_id,
student_id,
club_name,
membership_type,
join_date,
annual_fee
FROM community.membership_temporary
ORDER BY membership_id, student_id, loaded_at DESC
ON CONFLICT (membership_id, student_id) DO NOTHING;

CREATE TABLE community.membership_temporary (
membership_id VARCHAR(20),
student_id INTEGER,
club_name VARCHAR(100),
membership_type VARCHAR(50),
join_date DATE,
annual_fee DECIMAL(8,2),
loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
